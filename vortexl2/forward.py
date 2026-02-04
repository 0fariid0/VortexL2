"""
VortexL2 Port Forward Management

Handles socat-based TCP port forwarding with systemd service management.
Each port forward gets its own service file with the correct remote IP.
"""

import subprocess
import re
from pathlib import Path
from typing import List, Tuple, Dict


SYSTEMD_DIR = Path("/etc/systemd/system")

# Service file template - one per port (not a systemd template)
SERVICE_TEMPLATE = """[Unit]
Description=VortexL2 Port Forward - {tunnel} - Port {port}
After=network.target
Requires=network.target

[Service]
Type=simple
ExecStart=/usr/bin/socat TCP4-LISTEN:{port},bind={listen_ip},reuseaddr,fork TCP4:{remote_ip}:{port}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
"""


def run_command(cmd: str) -> Tuple[bool, str]:
    """Execute a shell command and return (success, output)."""
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=30
        )
        output = result.stdout.strip() or result.stderr.strip()
        return result.returncode == 0, output
    except Exception as e:
        return False, str(e)


class ForwardManager:
    """Manages socat port forwarding services."""

    def __init__(self, config):
        self.config = config

    def _sanitize_unit_part(self, s: str) -> str:
        """Make a safe systemd unit name fragment."""
        s = (s or "").strip().lower()
        s = "".join(c if (c.isalnum() or c in "-_") else "-" for c in s)
        s = re.sub(r"-+", "-", s).strip("-")
        return s or "tunnel"

    def _legacy_service_name(self, port: int) -> str:
        """Old unit naming (pre multi-tunnel-safe)."""
        return f"vortexl2-fwd-{port}.service"

    def _get_service_name(self, port: int) -> str:
        """New unit naming includes tunnel name to avoid collisions."""
        tname = self._sanitize_unit_part(getattr(self.config, "name", "tunnel"))
        return f"vortexl2-fwd-{tname}-{port}.service"

    def _get_service_path(self, port: int) -> Path:
        return SYSTEMD_DIR / self._get_service_name(port)

    def _legacy_service_path(self, port: int) -> Path:
        return SYSTEMD_DIR / self._legacy_service_name(port)

    def _migrate_legacy_unit(self, port: int) -> None:
        """If an old-style unit exists for this port, stop/disable it and remove the file."""
        legacy_name = self._legacy_service_name(port)
        legacy_path = self._legacy_service_path(port)
        if legacy_path.exists():
            run_command(f"systemctl stop {legacy_name}")
            run_command(f"systemctl disable {legacy_name}")
            try:
                legacy_path.unlink()
            except Exception:
                pass
            run_command("systemctl daemon-reload")

    def create_forward(self, port: int) -> Tuple[bool, str]:
        """Create and start a port forward service."""
        remote_ip = getattr(self.config, "remote_forward_ip", None)
        if not remote_ip:
            return False, "Remote forward IP not configured"

        listen_ip = getattr(self.config, "listen_ip", "0.0.0.0")

        # Clean up legacy unit if present
        self._migrate_legacy_unit(port)

        service_path = self._get_service_path(port)
        service_name = self._get_service_name(port)
        tunnel_name = getattr(self.config, "name", "tunnel")

        service_content = SERVICE_TEMPLATE.format(
            tunnel=tunnel_name,
            port=port,
            remote_ip=remote_ip,
            listen_ip=listen_ip
        )

        try:
            with open(service_path, "w") as f:
                f.write(service_content)
        except Exception as e:
            return False, f"Failed to create service file: {e}"

        run_command("systemctl daemon-reload")

        success, output = run_command(f"systemctl enable --now {service_name}")
        if not success:
            return False, f"Failed to start forward for port {port}: {output}"

        # Persist in config
        self.config.add_port(port)

        return True, f"Port forward for {port} created (listen {listen_ip} -> {remote_ip}:{port})"

    def remove_forward(self, port: int) -> Tuple[bool, str]:
        """Stop, disable and remove a port forward service."""
        service_name = self._get_service_name(port)
        legacy_name = self._legacy_service_name(port)

        run_command(f"systemctl stop {service_name}")
        run_command(f"systemctl disable {service_name}")
        run_command(f"systemctl stop {legacy_name}")
        run_command(f"systemctl disable {legacy_name}")

        # Remove service files (new + legacy if any)
        for path in [self._get_service_path(port), self._legacy_service_path(port)]:
            if path.exists():
                try:
                    path.unlink()
                except Exception:
                    pass

        run_command("systemctl daemon-reload")

        self.config.remove_port(port)
        return True, f"Port forward for {port} removed"

    def add_multiple_forwards(self, ports_str: str) -> Tuple[bool, str]:
        """Add multiple port forwards from comma-separated string."""
        results = []
        ports = [p.strip() for p in ports_str.split(",") if p.strip()]

        for port_str in ports:
            try:
                port = int(port_str)
                success, msg = self.create_forward(port)
                results.append(f"Port {port}: {msg}")
            except ValueError:
                results.append(f"Port '{port_str}': Invalid port number")

        return True, "\n".join(results)

    def remove_multiple_forwards(self, ports_str: str) -> Tuple[bool, str]:
        """Remove multiple port forwards from comma-separated string."""
        results = []
        ports = [p.strip() for p in ports_str.split(",") if p.strip()]

        for port_str in ports:
            try:
                port = int(port_str)
                success, msg = self.remove_forward(port)
                results.append(f"Port {port}: {msg}")
            except ValueError:
                results.append(f"Port '{port_str}': Invalid port number")

        return True, "\n".join(results)

    def list_forwards(self) -> List[Dict]:
        """List all configured port forwards with their status."""
        forwards = []

        for port in getattr(self.config, "forwarded_ports", []):
            new_unit = self._get_service_name(port)
            legacy_unit = self._legacy_service_name(port)

            # Prefer new service name; fall back to legacy
            success, output = run_command(f"systemctl is-active {new_unit}")
            if success:
                status = output
                active_unit = new_unit
            else:
                success2, output2 = run_command(f"systemctl is-active {legacy_unit}")
                status = output2 if success2 else "inactive"
                active_unit = legacy_unit if success2 else new_unit

            success, output = run_command(f"systemctl is-enabled {active_unit}")
            enabled = output if success else "disabled"

            remote_ip = getattr(self.config, "remote_forward_ip", "-")
            forwards.append({
                "port": port,
                "status": status,
                "enabled": enabled,
                "remote": f"{remote_ip}:{port}"
            })

        return forwards

    def start_all_forwards(self) -> Tuple[bool, str]:
        """Start all configured port forwards."""
        results = []

        for port in getattr(self.config, "forwarded_ports", []):
            # Ensure we are on the new unit naming scheme
            self._migrate_legacy_unit(port)

            service_name = self._get_service_name(port)
            service_path = self._get_service_path(port)

            if not service_path.exists():
                success, msg = self.create_forward(port)
                if success:
                    results.append(f"Port {port}: recreated and started")
                else:
                    results.append(f"Port {port}: failed to recreate - {msg}")
                continue

            success, output = run_command(f"systemctl start {service_name}")
            if success:
                results.append(f"Port {port}: started")
            else:
                results.append(f"Port {port}: failed to start - {output}")

        if not results:
            return True, "No port forwards configured"

        return True, "\n".join(results)

    def stop_all_forwards(self) -> Tuple[bool, str]:
        """Stop all configured port forwards (new + any legacy units)."""
        results = []

        for port in getattr(self.config, "forwarded_ports", []):
            new_unit = self._get_service_name(port)
            legacy_unit = self._legacy_service_name(port)

            success, output = run_command(f"systemctl stop {new_unit}")
            if success:
                results.append(f"Port {port}: stopped ({new_unit})")

            # Stop legacy too if it exists
            run_command(f"systemctl stop {legacy_unit}")

        if not results:
            return True, "No port forwards configured"

        return True, "\n".join(results)

    def restart_all_forwards(self) -> Tuple[bool, str]:
        """Restart all configured port forwards."""
        results = []

        for port in getattr(self.config, "forwarded_ports", []):
            # Clean up any old-style units for this port
            self._migrate_legacy_unit(port)

            service_name = self._get_service_name(port)
            service_path = self._get_service_path(port)

            if not service_path.exists():
                success, msg = self.create_forward(port)
                if success:
                    results.append(f"Port {port}: recreated and started")
                else:
                    results.append(f"Port {port}: failed to recreate - {msg}")
                continue

            remote_ip = getattr(self.config, "remote_forward_ip", None)
            if not remote_ip:
                results.append(f"Port {port}: remote_forward_ip not set")
                continue

            listen_ip = getattr(self.config, "listen_ip", "0.0.0.0")
            tunnel_name = getattr(self.config, "name", "tunnel")

            service_content = SERVICE_TEMPLATE.format(
                tunnel=tunnel_name,
                port=port,
                remote_ip=remote_ip,
                listen_ip=listen_ip
            )

            try:
                with open(service_path, "w") as f:
                    f.write(service_content)
            except Exception as e:
                results.append(f"Port {port}: failed to write service file - {e}")
                continue

            run_command("systemctl daemon-reload")
            success, output = run_command(f"systemctl restart {service_name}")
            if success:
                results.append(f"Port {port}: restarted")
            else:
                results.append(f"Port {port}: failed - {output}")

        if not results:
            return True, "No port forwards configured"

        return True, "\n".join(results)

    def install_template(self) -> Tuple[bool, str]:
        """Backward compatible no-op (older versions used a systemd template unit)."""
        return True, "No template required (per-port units)"
