#!/usr/bin/env python3
"""
Enhanced Server Inventory Discovery Tool
Collects comprehensive server information including:
- Service dependencies
- Application-to-application communication
- Port-level dependencies
- Previous day CPU utilization from sysstat
- Automatic systemd timer setup for scheduled collection
"""

import json
import platform
import socket
import subprocess
import os
import datetime
import psutil
import re
import shutil
from pathlib import Path
from collections import defaultdict

class ServerInventory:
    def __init__(self):
        self.inventory = {}
        self.service_port_map = {}  # Maps services to their ports
        self.port_service_map = {}  # Maps ports to services
        self.process_connections = defaultdict(list)  # Maps processes to their connections
    
    def setup_systemd_service_and_timer(self):
        """
        Create and enable systemd service and timer for server-inventory if they don't exist.
        Works on both RPM and DEB based systems.
        """
        result = {
            'service_created': False,
            'timer_created': False,
            'service_enabled': False,
            'timer_enabled': False,
            'timer_started': False,
            'error': None
        }
        
        try:
            # Check if systemd is available
            systemd_check = subprocess.run(
                ['systemctl', '--version'],
                capture_output=True,
                text=True
            )
            if systemd_check.returncode != 0:
                result['error'] = "systemd not available on this system"
                print(f"✗ {result['error']}")
                return result
            
            # Define systemd directory
            systemd_dir = Path('/etc/systemd/system')
            if not systemd_dir.exists():
                result['error'] = f"{systemd_dir} does not exist"
                print(f"✗ {result['error']}")
                return result
            
            service_file = systemd_dir / 'server-inventory.service'
            timer_file = systemd_dir / 'server-inventory.timer'
            
            # Determine the Python interpreter and script path
            python_exec = shutil.which('python3') or shutil.which('python')
            script_path = shutil.which('server-inventory') or '/usr/bin/server-inventory'
            
            # Service file content
            service_content = f"""[Unit]
Description=Server Inventory Collection Service
Documentation=man:server-inventory(1)
After=network-online.target sysstat.service
Wants=network-online.target

[Service]
Type=oneshot
ExecStart={script_path} -o /var/log/server-inventory/inventory-%Y%m%d-%H%M%S.json
User=root
Group=root

# Resource limits
MemoryLimit=512M
CPUQuota=50%%
TimeoutSec=600

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=server-inventory

# Security hardening
PrivateTmp=true
NoNewPrivileges=false
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/log/server-inventory

[Install]
WantedBy=multi-user.target
"""

            # Timer file content
            timer_content = """[Unit]
Description=Run Server Inventory Collection Every 6 Hours
Documentation=man:server-inventory(1)
Requires=server-inventory.service

[Timer]
# Run every 6 hours starting at midnight
OnCalendar=00/6:00:00

# Run on boot if a scheduled run was missed
Persistent=true

# Add random delay to prevent load spikes
RandomizedDelaySec=300

# Run 5 minutes after boot if system was down during scheduled time
OnBootSec=5min

[Install]
WantedBy=timers.target
"""

            # Create log directory if it doesn't exist
            log_dir = Path('/var/log/server-inventory')
            if not log_dir.exists():
                try:
                    log_dir.mkdir(parents=True, mode=0o755)
                    print(f"✓ Created log directory: {log_dir}")
                except Exception as e:
                    print(f"⚠ Warning: Could not create log directory {log_dir}: {e}")
            
            # Create or update service file
            if service_file.exists():
                print(f"✓ Service file already exists: {service_file}")
                # Optionally check if content matches and update if different
                with open(service_file, 'r') as f:
                    existing_content = f.read()
                if existing_content != service_content:
                    print("  Updating service file with new content...")
                    with open(service_file, 'w') as f:
                        f.write(service_content)
                    result['service_created'] = True
                    print(f"✓ Updated service file: {service_file}")
            else:
                with open(service_file, 'w') as f:
                    f.write(service_content)
                service_file.chmod(0o644)
                result['service_created'] = True
                print(f"✓ Created service file: {service_file}")
            
            # Create or update timer file
            if timer_file.exists():
                print(f"✓ Timer file already exists: {timer_file}")
                with open(timer_file, 'r') as f:
                    existing_content = f.read()
                if existing_content != timer_content:
                    print("  Updating timer file with new content...")
                    with open(timer_file, 'w') as f:
                        f.write(timer_content)
                    result['timer_created'] = True
                    print(f"✓ Updated timer file: {timer_file}")
            else:
                with open(timer_file, 'w') as f:
                    f.write(timer_content)
                timer_file.chmod(0o644)
                result['timer_created'] = True
                print(f"✓ Created timer file: {timer_file}")
            
            # Reload systemd daemon
            reload_result = subprocess.run(
                ['systemctl', 'daemon-reload'],
                capture_output=True,
                text=True
            )
            if reload_result.returncode == 0:
                print("✓ Reloaded systemd daemon")
            else:
                print(f"⚠ Warning: Failed to reload systemd daemon: {reload_result.stderr}")
            
            # Enable the timer (this will also enable the service as a dependency)
            enable_result = subprocess.run(
                ['systemctl', 'enable', 'server-inventory.timer'],
                capture_output=True,
                text=True
            )
            
            if enable_result.returncode == 0:
                result['timer_enabled'] = True
                result['service_enabled'] = True
                print("✓ Enabled server-inventory.timer")
            else:
                # Check if already enabled
                is_enabled = subprocess.run(
                    ['systemctl', 'is-enabled', 'server-inventory.timer'],
                    capture_output=True,
                    text=True
                )
                if is_enabled.returncode == 0:
                    result['timer_enabled'] = True
                    print("✓ Timer already enabled")
                else:
                    print(f"⚠ Warning: Could not enable timer: {enable_result.stderr}")
            
            # Start the timer
            start_result = subprocess.run(
                ['systemctl', 'start', 'server-inventory.timer'],
                capture_output=True,
                text=True
            )
            
            if start_result.returncode == 0:
                result['timer_started'] = True
                print("✓ Started server-inventory.timer")
            else:
                # Check if already running
                is_active = subprocess.run(
                    ['systemctl', 'is-active', 'server-inventory.timer'],
                    capture_output=True,
                    text=True
                )
                if is_active.returncode == 0:
                    result['timer_started'] = True
                    print("✓ Timer already running")
                else:
                    print(f"⚠ Warning: Could not start timer: {start_result.stderr}")
            
            # Display status and next run time
            print("\n" + "="*60)
            print("SYSTEMD TIMER SETUP COMPLETE")
            print("="*60)
            
            # Show timer status
            status_result = subprocess.run(
                ['systemctl', 'list-timers', 'server-inventory.timer', '--no-pager'],
                capture_output=True,
                text=True
            )
            if status_result.returncode == 0:
                print("\nTimer Schedule:")
                print(status_result.stdout)
            
            print("\nUseful Commands:")
            print("  Check timer status:  systemctl status server-inventory.timer")
            print("  Check service logs:  journalctl -u server-inventory -n 50")
            print("  Run immediately:     systemctl start server-inventory.service")
            print("  Stop timer:          systemctl stop server-inventory.timer")
            print("  Disable timer:       systemctl disable server-inventory.timer")
            print("="*60 + "\n")
            
        except PermissionError:
            result['error'] = "Permission denied. Run with sudo/root privileges."
            print(f"✗ {result['error']}")
        except FileNotFoundError as e:
            result['error'] = f"Required command not found: {e}"
            print(f"✗ {result['error']}")
        except Exception as e:
            result['error'] = f"Unexpected error: {str(e)}"
            print(f"✗ {result['error']}")
        
        return result
    
    def check_and_install_sysstat(self):
        """Check if sysstat service is enabled, and enable/start it if not"""
        result = {
            'service_active': False,
            'service_enabled': False,
            'service_started': False,
            'debian_config_updated': False,
            'error': None
        }
        
        try:
            # Check if sysstat service exists
            check_service = subprocess.run(
                ['systemctl', 'list-unit-files', 'sysstat.service'],
                capture_output=True,
                text=True
            )
            
            if 'sysstat.service' not in check_service.stdout:
                result['error'] = "sysstat service not found. Please install sysstat package first."
                print(f"✗ {result['error']}")
                return result
            
            # Check if service is already active
            status_check = subprocess.run(
                ['systemctl', 'is-active', 'sysstat'],
                capture_output=True,
                text=True
            )
            
            if status_check.returncode == 0:
                result['service_active'] = True
                print("✓ sysstat service is already active")
            else:
                print("✗ sysstat service is not active")
            
            # For Debian/Ubuntu, enable sysstat in /etc/default/sysstat
            if os.path.exists('/etc/default/sysstat'):
                print("Configuring sysstat for Debian/Ubuntu...")
                try:
                    with open('/etc/default/sysstat', 'r') as f:
                        content = f.read()
                    
                    # Enable sysstat if it's disabled
                    if 'ENABLED="false"' in content:
                        content = content.replace('ENABLED="false"', 'ENABLED="true"')
                        with open('/etc/default/sysstat', 'w') as f:
                            f.write(content)
                        result['debian_config_updated'] = True
                        print("✓ Enabled sysstat in /etc/default/sysstat")
                    else:
                        print("✓ sysstat already enabled in /etc/default/sysstat")
                        
                except Exception as e:
                    print(f"Warning: Could not modify /etc/default/sysstat: {e}")
            
            # Enable the service to start on boot
            enable_result = subprocess.run(
                ['systemctl', 'enable', 'sysstat'],
                capture_output=True,
                text=True
            )
            
            if enable_result.returncode == 0:
                result['service_enabled'] = True
                print("✓ sysstat service enabled to start on boot")
            else:
                # Check if already enabled
                is_enabled = subprocess.run(
                    ['systemctl', 'is-enabled', 'sysstat'],
                    capture_output=True,
                    text=True
                )
                if is_enabled.returncode == 0:
                    result['service_enabled'] = True
                    print("✓ sysstat service already enabled")
                else:
                    print(f"Warning: Could not enable sysstat service: {enable_result.stderr}")
            
            # Start the service
            start_result = subprocess.run(
                ['systemctl', 'start', 'sysstat'],
                capture_output=True,
                text=True
            )
            
            if start_result.returncode == 0:
                result['service_started'] = True
                print("✓ sysstat service started")
            else:
                # Check if already running
                is_active = subprocess.run(
                    ['systemctl', 'is-active', 'sysstat'],
                    capture_output=True,
                    text=True
                )
                if is_active.returncode == 0:
                    result['service_started'] = True
                    result['service_active'] = True
                    print("✓ sysstat service already running")
                else:
                    print(f"Warning: Could not start sysstat service: {start_result.stderr}")
            
            # Final verification
            final_check = subprocess.run(
                ['systemctl', 'is-active', 'sysstat'],
                capture_output=True,
                text=True
            )
            
            if final_check.returncode == 0:
                print("✓ sysstat service is now active and running")
                result['service_active'] = True
            else:
                print("⚠ sysstat service configuration completed but service may need manual start")
                
        except FileNotFoundError:
            result['error'] = "systemctl not found. This system may not use systemd."
            print(f"✗ {result['error']}")
        except PermissionError:
            result['error'] = "Permission denied. Run with sudo/root privileges."
            print(f"✗ {result['error']}")
        except Exception as e:
            result['error'] = f"Unexpected error: {str(e)}"
            print(f"✗ {result['error']}")
        
        return result
    

    def get_system_info(self):
        """Gather basic system information"""
        return {
            'hostname': socket.gethostname(),
            'fqdn': socket.getfqdn(),
            'platform': platform.system(),
            'platform_release': platform.release(),
            'platform_version': platform.version(),
            'architecture': platform.machine(),
            'processor': platform.processor(),
            'python_version': platform.python_version(),
        }
    
    def get_os_info(self):
        """Get detailed OS information"""
        os_info = {}
        
        # Read /etc/os-release
        if os.path.exists('/etc/os-release'):
            with open('/etc/os-release', 'r') as f:
                for line in f:
                    if '=' in line:
                        key, value = line.strip().split('=', 1)
                        os_info[key.lower()] = value.strip('"')
        
        # Get kernel info
        os_info['kernel_version'] = platform.release()
        
        # Get uptime
        try:
            with open('/proc/uptime', 'r') as f:
                uptime_seconds = float(f.readline().split()[0])
                os_info['uptime_seconds'] = uptime_seconds
                os_info['uptime_days'] = uptime_seconds / 86400
        except:
            pass
        
        return os_info
    
    def get_cpu_info(self):
        """Gather CPU information"""
        cpu_info = {
            'physical_cores': psutil.cpu_count(logical=False),
            'logical_cores': psutil.cpu_count(logical=True),
            'max_frequency_mhz': psutil.cpu_freq().max if psutil.cpu_freq() else None,
            'min_frequency_mhz': psutil.cpu_freq().min if psutil.cpu_freq() else None,
            'current_frequency_mhz': psutil.cpu_freq().current if psutil.cpu_freq() else None,
            'cpu_percent': psutil.cpu_percent(interval=1),
            'per_cpu_percent': psutil.cpu_percent(interval=1, percpu=True)
        }
        
        # Read /proc/cpuinfo for detailed info
        if os.path.exists('/proc/cpuinfo'):
            with open('/proc/cpuinfo', 'r') as f:
                cpuinfo = f.read()
                for line in cpuinfo.split('\n'):
                    if 'model name' in line:
                        cpu_info['model_name'] = line.split(':')[1].strip()
                        break
        
        return cpu_info
    
    def get_previous_day_cpu_utilization(self):
        """Get previous day CPU utilization from sysstat/sar"""
        cpu_history = {
            'available': False,
            'date': None,
            'average': {},
            'hourly_data': [],
            'raw_output': None,
            'error': None
        }
        
        try:
            # Get yesterday's date in the format needed for sa file
            yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
            sa_file_day = yesterday.strftime('%d')
            if shutil.which("rpm"):
                sa_file_path = f'/var/log/sa/sa{sa_file_day}'
            else:
                sa_file_path = f'/var/log/sysstat/sa{sa_file_day}'
            # Check if the file exists
            if not os.path.exists(sa_file_path):
                cpu_history['error'] = f'Sysstat file not found: {sa_file_path}'
                return cpu_history
            
            # Run sar command to get CPU utilization
            result = subprocess.run(
                ['sar', '-u', '-f', sa_file_path],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                cpu_history['available'] = True
                cpu_history['date'] = yesterday.strftime('%Y-%m-%d')
                cpu_history['raw_output'] = result.stdout
                
                # Parse the sar output
                lines = result.stdout.strip().split('\n')
                data_started = False
                
                for line in lines:
                    line = line.strip()
                    
                    # Skip empty lines
                    if not line:
                        continue
                    
                    # Check for Average line
                    if line.startswith('Average:'):
                        parts = line.split()
                        if len(parts) >= 8:
                            cpu_history['average'] = {
                                'user': float(parts[2]),
                                'nice': float(parts[3]),
                                'system': float(parts[4]),
                                'iowait': float(parts[5]),
                                'steal': float(parts[6]),
                                'idle': float(parts[7])
                            }
                    
                    # Parse data lines (time stamps)
                    elif ':' in line and not line.startswith('Linux') and not line.startswith('Average'):
                        parts = line.split()
                        # Check if this looks like a data line (has time in first or second column)
                        if len(parts) >= 8 and (':' in parts[0] or ':' in parts[1]):
                            try:
                                time_col = 0 if ':' in parts[0] else 1
                                cpu_history['hourly_data'].append({
                                    'time': parts[time_col],
                                    'cpu': parts[time_col + 1] if parts[time_col + 1] != 'all' else 'all',
                                    'user': float(parts[time_col + 2]),
                                    'nice': float(parts[time_col + 3]),
                                    'system': float(parts[time_col + 4]),
                                    'iowait': float(parts[time_col + 5]),
                                    'steal': float(parts[time_col + 6]),
                                    'idle': float(parts[time_col + 7])
                                })
                            except (ValueError, IndexError):
                                # Skip lines that don't match expected format
                                continue
            else:
                cpu_history['error'] = f'sar command failed: {result.stderr}'
                
        except FileNotFoundError:
            cpu_history['error'] = 'sar command not found. Install sysstat package.'
        except subprocess.TimeoutExpired:
            cpu_history['error'] = 'sar command timed out'
        except Exception as e:
            cpu_history['error'] = f'Error collecting previous day CPU data: {str(e)}'
        
        return cpu_history
    
    def get_memory_info(self):
        """Gather memory information"""
        mem = psutil.virtual_memory()
        swap = psutil.swap_memory()
        
        return {
            'total_bytes': mem.total,
            'total_gb': round(mem.total / (1024**3), 2),
            'available_bytes': mem.available,
            'available_gb': round(mem.available / (1024**3), 2),
            'used_bytes': mem.used,
            'used_gb': round(mem.used / (1024**3), 2),
            'percent_used': mem.percent,
            'swap_total_gb': round(swap.total / (1024**3), 2),
            'swap_used_gb': round(swap.used / (1024**3), 2),
            'swap_percent': swap.percent
        }
    
    def get_disk_info(self):
        """Gather disk and filesystem information"""
        disks = []
        
        for partition in psutil.disk_partitions():
            try:
                usage = psutil.disk_usage(partition.mountpoint)
                disks.append({
                    'device': partition.device,
                    'mountpoint': partition.mountpoint,
                    'filesystem_type': partition.fstype,
                    'options': partition.opts,
                    'total_bytes': usage.total,
                    'total_gb': round(usage.total / (1024**3), 2),
                    'used_bytes': usage.used,
                    'used_gb': round(usage.used / (1024**3), 2),
                    'free_bytes': usage.free,
                    'free_gb': round(usage.free / (1024**3), 2),
                    'percent_used': usage.percent
                })
            except PermissionError:
                continue
        
        return disks
    
    def get_network_info(self):
        """Gather network information"""
        network_info = {
            'interfaces': {},
            'connections': [],
            'listening_ports': [],
            'established_connections': []
        }
        
        # Network interfaces
        for interface, addrs in psutil.net_if_addrs().items():
            network_info['interfaces'][interface] = []
            for addr in addrs:
                network_info['interfaces'][interface].append({
                    'family': str(addr.family),
                    'address': addr.address,
                    'netmask': addr.netmask,
                    'broadcast': addr.broadcast
                })
        
        # Network statistics
        net_io = psutil.net_io_counters()
        network_info['statistics'] = {
            'bytes_sent': net_io.bytes_sent,
            'bytes_recv': net_io.bytes_recv,
            'packets_sent': net_io.packets_sent,
            'packets_recv': net_io.packets_recv,
            'error_in': net_io.errin,
            'error_out': net_io.errout,
            'drop_in': net_io.dropin,
            'drop_out': net_io.dropout
        }
        
        # Get all network connections
        try:
            connections = psutil.net_connections(kind='inet')
            listening_ports = {}
            established = []
            
            for conn in connections:
                conn_info = {
                    'fd': conn.fd,
                    'family': str(conn.family),
                    'type': str(conn.type),
                    'local_address': f"{conn.laddr.ip}:{conn.laddr.port}" if conn.laddr else None,
                    'remote_address': f"{conn.raddr.ip}:{conn.raddr.port}" if conn.raddr else None,
                    'status': conn.status,
                    'pid': conn.pid
                }
                
                # Get process name for the connection
                if conn.pid:
                    try:
                        proc = psutil.Process(conn.pid)
                        conn_info['process_name'] = proc.name()
                        conn_info['process_user'] = proc.username()
                        conn_info['process_cmdline'] = ' '.join(proc.cmdline())
                        
                        # Store for application communication mapping
                        self.process_connections[conn.pid].append(conn_info)
                        
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        conn_info['process_name'] = None
                        conn_info['process_user'] = None
                
                # Categorize listening ports
                if conn.status == 'LISTEN':
                    port = conn.laddr.port
                    protocol = 'TCP' if conn.type == socket.SOCK_STREAM else 'UDP'
                    
                    if port not in listening_ports:
                        service_info = {
                            'port': port,
                            'protocol': protocol,
                            'address': conn.laddr.ip,
                            'pid': conn.pid,
                            'process_name': conn_info.get('process_name'),
                            'process_user': conn_info.get('process_user'),
                            'process_cmdline': conn_info.get('process_cmdline')
                        }
                        listening_ports[port] = service_info
                        
                        # Map port to service for dependency tracking
                        if conn_info.get('process_name'):
                            self.port_service_map[port] = {
                                'service': conn_info.get('process_name'),
                                'pid': conn.pid
                            }
                            if conn_info.get('process_name') not in self.service_port_map:
                                self.service_port_map[conn_info.get('process_name')] = []
                            self.service_port_map[conn_info.get('process_name')].append(port)
                
                # Categorize established connections
                elif conn.status == 'ESTABLISHED':
                    established.append(conn_info)
                
                network_info['connections'].append(conn_info)
            
            # Convert listening_ports dict to sorted list
            network_info['listening_ports'] = sorted(
                listening_ports.values(),
                key=lambda x: x['port']
            )
            network_info['established_connections'] = established
            
        except (psutil.AccessDenied, PermissionError):
            print("Warning: Need root privileges for complete network connection info")
        
        return network_info
    
    def get_service_dependencies(self):
        """
        Analyze systemd service dependencies
        Returns service dependency tree with Requires, Wants, After, Before relationships
        """
        dependencies = {}
        
        try:
            # Get list of all services
            result = subprocess.run(
                ['systemctl', 'list-unit-files', '--type=service', '--no-pager'],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            services = []
            for line in result.stdout.split('\n'):
                if '.service' in line:
                    parts = line.split()
                    if parts:
                        services.append(parts[0])
            
            # For each service, get its dependencies
            for service in services[:100]:  # Limit to avoid timeout
                try:
                    service_deps = {
                        'service_name': service,
                        'status': None,
                        'requires': [],
                        'wants': [],
                        'required_by': [],
                        'wanted_by': [],
                        'after': [],
                        'before': [],
                        'conflicts': [],
                        'unit_file_path': None,
                        'description': None
                    }
                    
                    # Get service status
                    status_result = subprocess.run(
                        ['systemctl', 'is-active', service],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    service_deps['status'] = status_result.stdout.strip()
                    
                    # Get detailed service info
                    show_result = subprocess.run(
                        ['systemctl', 'show', service, '--no-pager'],
                        capture_output=True,
                        text=True,
                        timeout=10
                    )
                    
                    for line in show_result.stdout.split('\n'):
                        if '=' in line:
                            key, value = line.split('=', 1)
                            if key == 'Requires':
                                service_deps['requires'] = [s.strip() for s in value.split() if s.strip()]
                            elif key == 'Wants':
                                service_deps['wants'] = [s.strip() for s in value.split() if s.strip()]
                            elif key == 'After':
                                service_deps['after'] = [s.strip() for s in value.split() if s.strip()]
                            elif key == 'Before':
                                service_deps['before'] = [s.strip() for s in value.split() if s.strip()]
                            elif key == 'Conflicts':
                                service_deps['conflicts'] = [s.strip() for s in value.split() if s.strip()]
                            elif key == 'FragmentPath':
                                service_deps['unit_file_path'] = value.strip()
                            elif key == 'Description':
                                service_deps['description'] = value.strip()
                    
                    # Get reverse dependencies (what depends on this service)
                    list_deps = subprocess.run(
                        ['systemctl', 'list-dependencies', '--reverse', service, '--no-pager'],
                        capture_output=True,
                        text=True,
                        timeout=10
                    )
                    
                    for line in list_deps.stdout.split('\n'):
                        line = line.strip()
                        if '.service' in line:
                            # Clean up the tree characters
                            clean_line = re.sub(r'[│├└─\s●]+', '', line)
                            if clean_line and clean_line != service:
                                service_deps['required_by'].append(clean_line)
                    
                    dependencies[service] = service_deps
                    
                except subprocess.TimeoutExpired:
                    continue
                except Exception as e:
                    continue
                    
        except Exception as e:
            print(f"Error collecting service dependencies: {e}")
        
        return dependencies
    
    def get_application_communication_map(self):
        """
        Map application-to-application communication based on network connections
        Shows which processes communicate with which services/hosts
        """
        comm_map = {
            'process_to_service': {},  # Local process to local service
            'process_to_external': {},  # Local process to external host
            'service_clients': {},      # Services and their clients
            'communication_matrix': []
        }
        
        try:
            # Analyze established connections
            for pid, connections in self.process_connections.items():
                try:
                    proc = psutil.Process(pid)
                    proc_name = proc.name()
                    proc_cmdline = ' '.join(proc.cmdline())
                    
                    if proc_name not in comm_map['process_to_service']:
                        comm_map['process_to_service'][proc_name] = {
                            'pid': pid,
                            'cmdline': proc_cmdline,
                            'local_connections': [],
                            'external_connections': []
                        }
                    
                    for conn in connections:
                        if conn['status'] == 'ESTABLISHED' and conn['remote_address']:
                            remote_ip, remote_port = conn['remote_address'].split(':')
                            remote_port = int(remote_port)
                            
                            connection_detail = {
                                'remote_host': remote_ip,
                                'remote_port': remote_port,
                                'local_port': conn['local_address'].split(':')[1] if conn['local_address'] else None,
                                'protocol': 'TCP' if 'STREAM' in conn['type'] else 'UDP'
                            }
                            
                            # Check if connecting to local service
                            if remote_ip in ['127.0.0.1', '::1', 'localhost'] or remote_ip.startswith('127.'):
                                # Connecting to local service
                                target_service = self.port_service_map.get(remote_port, {}).get('service', f'unknown-port-{remote_port}')
                                connection_detail['target_service'] = target_service
                                connection_detail['connection_type'] = 'local'
                                comm_map['process_to_service'][proc_name]['local_connections'].append(connection_detail)
                                
                                # Track service clients
                                if target_service not in comm_map['service_clients']:
                                    comm_map['service_clients'][target_service] = []
                                if proc_name not in [c['client'] for c in comm_map['service_clients'][target_service]]:
                                    comm_map['service_clients'][target_service].append({
                                        'client': proc_name,
                                        'client_pid': pid,
                                        'port': remote_port
                                    })
                                
                                # Add to communication matrix
                                comm_map['communication_matrix'].append({
                                    'source_process': proc_name,
                                    'source_pid': pid,
                                    'target_service': target_service,
                                    'target_port': remote_port,
                                    'connection_type': 'local',
                                    'protocol': connection_detail['protocol']
                                })
                            else:
                                # External connection
                                connection_detail['connection_type'] = 'external'
                                connection_detail['service_name'] = self._identify_service_by_port(remote_port)
                                comm_map['process_to_service'][proc_name]['external_connections'].append(connection_detail)
                                
                                comm_map['communication_matrix'].append({
                                    'source_process': proc_name,
                                    'source_pid': pid,
                                    'target_host': remote_ip,
                                    'target_port': remote_port,
                                    'connection_type': 'external',
                                    'protocol': connection_detail['protocol'],
                                    'likely_service': connection_detail['service_name']
                                })
                
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
        
        except Exception as e:
            print(f"Error mapping application communication: {e}")
        
        return comm_map
    
    def get_port_dependencies(self):
        """
        Analyze port-level dependencies
        Shows which services listen on which ports and what connects to them

        Note:
        - Requires root privileges for complete visibility
        - IPv4 / IPv6 safe (Debian, Ubuntu, RHEL)
        """

        port_deps = {
            'listening_services': {},
            'port_usage_map': {},
            'well_known_ports': {},
            'dependency_graph': []
        }

        # ------------------------------------------------------------------
        # Map listening ports to services
        # ------------------------------------------------------------------
        for port, service_info in self.port_service_map.items():
            port_deps['listening_services'][port] = {
                'service': service_info.get('service'),
                'pid': service_info.get('pid'),
                'well_known_service': self._identify_service_by_port(port),
                'clients': []
            }

        # ------------------------------------------------------------------
        # Find which processes connect to which ports
        # ------------------------------------------------------------------
        for pid, connections in self.process_connections.items():
            try:
                proc = psutil.Process(pid)
                proc_name = proc.name()

                for conn in connections:
                    if conn.get('status') != 'ESTABLISHED':
                        continue

                    remote_addr = conn.get('remote_address')
                    if not remote_addr:
                        continue

                    # -------------------------------
                    # Debian / IPv6 safe parsing
                    # -------------------------------
                    if isinstance(remote_addr, tuple):
                        remote_ip, remote_port = remote_addr

                    else:
                        # Handle [::1]:5432
                        if remote_addr.startswith('['):
                            remote_ip, remote_port = remote_addr[1:].rsplit(']:', 1)
                        else:
                            # Safe for IPv4 and IPv6
                            remote_ip, remote_port = remote_addr.rsplit(':', 1)

                        remote_port = int(remote_port)

                    # -------------------------------
                    # Local service dependency only
                    # -------------------------------
                    if remote_ip == '::1' or remote_ip.startswith('127.'):
                        if remote_port in port_deps['listening_services']:
                            client_info = {
                                'client_process': proc_name,
                                'client_pid': pid
                            }

                            clients = port_deps['listening_services'][remote_port]['clients']
                            if client_info not in clients:
                                clients.append(client_info)

                            # Dependency graph edge
                            port_deps['dependency_graph'].append({
                                'client_process': proc_name,
                                'client_pid': pid,
                                'server_port': remote_port,
                                'server_service': port_deps['listening_services'][remote_port]['service'],
                                'dependency_type': 'port-level'
                            })

            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        # ------------------------------------------------------------------
        # Create port usage summary
        # ------------------------------------------------------------------
        for port, info in port_deps['listening_services'].items():
            port_deps['port_usage_map'][port] = {
                'service': info['service'],
                'well_known_name': info['well_known_service'],
                'client_count': len(info['clients']),
                'clients': [c['client_process'] for c in info['clients']]
            }

        return port_deps

    
    def _identify_service_by_port(self, port):
        """Identify common service by port number"""
        common_ports = {
            20: 'FTP-DATA',
            21: 'FTP',
            22: 'SSH',
            23: 'Telnet',
            25: 'SMTP',
            53: 'DNS',
            80: 'HTTP',
            110: 'POP3',
            143: 'IMAP',
            443: 'HTTPS',
            445: 'SMB',
            587: 'SMTP-Submission',
            993: 'IMAPS',
            995: 'POP3S',
            1433: 'MS-SQL',
            1521: 'Oracle-DB',
            3306: 'MySQL',
            5432: 'PostgreSQL',
            5672: 'AMQP/RabbitMQ',
            6379: 'Redis',
            8080: 'HTTP-Alt',
            8443: 'HTTPS-Alt',
            9200: 'Elasticsearch',
            27017: 'MongoDB',
            11211: 'Memcached',
            50000: 'DB2'
        }
        return common_ports.get(int(port), f'Unknown-{port}')
    
    def get_docker_info(self):
        """Get Docker container information and networking"""
        docker_info = {
            'installed': False,
            'running': False,
            'containers': [],
            'networks': [],
            'container_ports': {}
        }
        
        try:
            # Check if Docker is installed
            result = subprocess.run(
                ['docker', '--version'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                docker_info['installed'] = True
                docker_info['version'] = result.stdout.strip()
            
            # Get running containers
            result = subprocess.run(
                ['sudo', 'docker', 'ps', '--format', '{{json .}}'],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                docker_info['running'] = True
                for line in result.stdout.strip().split('\n'):
                    if line:
                        try:
                            container = json.loads(line)
                            docker_info['containers'].append(container)
                        except:
                            pass
            
            # Get Docker networks
            result = subprocess.run(
                ['sudo', 'docker', 'network', 'ls', '--format', '{{json .}}'],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode == 0:
                for line in result.stdout.strip().split('\n'):
                    if line:
                        try:
                            network = json.loads(line)
                            docker_info['networks'].append(network)
                        except:
                            pass
            
            # Get port mappings for each container
            for container in docker_info['containers']:
                container_id = container.get('ID', '')
                if container_id:
                    result = subprocess.run(
                        ['sudo', 'docker', 'port', container_id],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    if result.returncode == 0:
                        docker_info['container_ports'][container.get('Names', '')] = result.stdout.strip()
        
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass
        
        return docker_info
    

    def get_installed_packages(self):
        """
        Get list of NON-OS (explicitly user-installed) packages.
        Supports:
        - Debian / Ubuntu (apt)
        - RHEL / CentOS 7 (yum)
        - RHEL / CentOS 8+ / Rocky / Alma (dnf)
        """

        packages = []

        # ======================================================
        # Debian / Ubuntu
        # ======================================================
        if shutil.which("dpkg") and shutil.which("apt-mark"):
            try:
                # Get explicitly installed packages
                manual = subprocess.run(
                    ["apt-mark", "showmanual"],
                    capture_output=True,
                    text=True,
                    timeout=30,
                    check=True
                )
                manual_packages = set(line.strip() for line in manual.stdout.splitlines() if line.strip())

                # Get all installed packages
                dpkg = subprocess.run(
                    ["dpkg", "-l"],
                    capture_output=True,
                    text=True,
                    timeout=30,
                    check=True
                )

                for line in dpkg.stdout.splitlines():
                    if line.startswith("ii"):
                        parts = line.split()
                        if len(parts) >= 3:
                            name = parts[1]
                            if name in manual_packages:
                                packages.append({
                                    "name": name,
                                    "version": parts[2],
                                    "architecture": parts[3] if len(parts) > 3 else None,
                                    "source": "apt"
                                })

                return packages

            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                # If apt commands fail, continue to try other package managers
                pass

        # ======================================================
        # RHEL / CentOS / Rocky / Alma
        # ======================================================
        user_installed = set()

        # ---- DNF (RHEL 8+) ----
        if shutil.which("dnf"):
            try:
                r = subprocess.run(
                    ["dnf", "history", "userinstalled"],
                    capture_output=True,
                    text=True,
                    timeout=30,
                    check=True
                )
                # Parse DNF output - skip header lines and extract package names
                for line in r.stdout.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    # Skip header lines (they don't contain dots or version info)
                    if line.startswith("Packages installed") or line.startswith("Last argument"):
                        continue
                    # Extract just the package name (before version/arch info)
                    # Format: package-name-version-release.arch
                    # We want just: package-name
                    parts = line.split()
                    if parts:
                        pkg_full = parts[0]
                        # Extract base package name from full NEVRA
                        # Example: containerd.io-2.2.1-1.el9.x86_64 -> containerd.io
                        match = re.match(r'^(.+?)-\d', pkg_full)
                        if match:
                            pkg_name = match.group(1)
                            user_installed.add(pkg_name)
                        else:
                            # Fallback: just use the full name if pattern doesn't match
                            user_installed.add(pkg_full)
                            
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                try:
                    r = subprocess.run(
                        ["dnf", "repoquery", "--userinstalled"],
                        capture_output=True,
                        text=True,
                        timeout=30,
                        check=True
                    )
                    for line in r.stdout.splitlines():
                        line = line.strip()
                        if line:
                            # Extract package name from NEVRA format
                            match = re.match(r'^(.+?)-\d', line)
                            if match:
                                user_installed.add(match.group(1))
                            else:
                                user_installed.add(line)
                except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                    pass

        # ---- YUM (CentOS 7) ----
        if not user_installed and shutil.which("yum"):
            try:
                r = subprocess.run(
                    ["yum", "history", "userinstalled"],
                    capture_output=True,
                    text=True,
                    timeout=30,
                    check=True
                )
                for line in r.stdout.splitlines():
                    line = line.strip()
                    if not line or line.startswith("Packages installed"):
                        continue
                    parts = line.split()
                    if parts:
                        pkg_full = parts[0]
                        match = re.match(r'^(.+?)-\d', pkg_full)
                        if match:
                            user_installed.add(match.group(1))
                        else:
                            user_installed.add(pkg_full)
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                pass

        # ---- RPM fallback (best effort) ----
        if shutil.which("rpm"):
            try:
                rpm = subprocess.run(
                    ["rpm", "-qa", "--qf", "%{NAME} %{VERSION}-%{RELEASE} %{ARCH}\n"],
                    capture_output=True,
                    text=True,
                    timeout=30,
                    check=True
                )

                for line in rpm.stdout.splitlines():
                    if not line.strip():
                        continue
                        
                    parts = line.split()
                    if len(parts) >= 3:
                        name = parts[0]
                        version = parts[1]
                        arch = parts[2]

                        if user_installed:
                            # Check if this package name is in our user-installed set
                            if name not in user_installed:
                                continue
                            source = "dnf/yum"
                        else:
                            # No history available - include all packages
                            source = "rpm-unknown"

                        packages.append({
                            "name": name,
                            "version": version,
                            "architecture": arch,
                            "source": source
                        })

            except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                pass

        return packages


    def get_firewall_info(self):
        """Get firewall and iptables information with inbound/outbound rules"""
        firewall_info = {
            'iptables': {
                'input_rules': [],
                'output_rules': [],
                'forward_rules': [],
                'nat_rules': [],
                'raw_output': None
            },
            'ip6tables': {
                'input_rules': [],
                'output_rules': [],
                'forward_rules': [],
                'raw_output': None
            },
            'ufw': {
                'status': None,
                'rules': [],
                'default_policies': {}
            },
            'firewalld': {
                'status': None,
                'zones': [],
                'services': [],
                'ports': []
            }
        }
        
        # Get iptables INPUT rules (inbound)
        try:
            result = subprocess.run(
                ['iptables', '-L', 'INPUT', '-n', '-v', '--line-numbers'],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0:
                firewall_info['iptables']['input_rules'] = self._parse_iptables_output(result.stdout)
        except:
            pass
        
        # Get iptables OUTPUT rules (outbound)
        try:
            result = subprocess.run(
                ['iptables', '-L', 'OUTPUT', '-n', '-v', '--line-numbers'],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0:
                firewall_info['iptables']['output_rules'] = self._parse_iptables_output(result.stdout)
        except:
            pass
        
        # Get iptables FORWARD rules
        try:
            result = subprocess.run(
                ['iptables', '-L', 'FORWARD', '-n', '-v', '--line-numbers'],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0:
                firewall_info['iptables']['forward_rules'] = self._parse_iptables_output(result.stdout)
        except:
            pass
        
        # Get NAT table rules
        try:
            result = subprocess.run(
                ['iptables', '-t', 'nat', '-L', '-n', '-v', '--line-numbers'],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0:
                firewall_info['iptables']['nat_rules'] = result.stdout
        except:
            pass
        
        # Get complete iptables dump
        try:
            result = subprocess.run(
                ['iptables-save'],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0:
                firewall_info['iptables']['raw_output'] = result.stdout
        except:
            pass
        
        # Get ip6tables rules (IPv6)
        try:
            result = subprocess.run(
                ['ip6tables', '-L', 'INPUT', '-n', '-v', '--line-numbers'],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0:
                firewall_info['ip6tables']['input_rules'] = self._parse_iptables_output(result.stdout)
        except:
            pass
        
        try:
            result = subprocess.run(
                ['ip6tables', '-L', 'OUTPUT', '-n', '-v', '--line-numbers'],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0:
                firewall_info['ip6tables']['output_rules'] = self._parse_iptables_output(result.stdout)
        except:
            pass
        
        try:
            result = subprocess.run(
                ['ip6tables-save'],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0:
                firewall_info['ip6tables']['raw_output'] = result.stdout
        except:
            pass
        
        # Check UFW status and rules
        try:
            result = subprocess.run(
                ['ufw', 'status', 'verbose'],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0:
                firewall_info['ufw']['status'] = result.stdout
                firewall_info['ufw']['rules'] = self._parse_ufw_rules(result.stdout)
        except:
            pass
        
        # Get UFW numbered rules for better parsing
        try:
            result = subprocess.run(
                ['ufw', 'status', 'numbered'],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0:
                firewall_info['ufw']['numbered_rules'] = result.stdout
        except:
            pass
        
        # Check firewalld status
        try:
            result = subprocess.run(
                ['firewall-cmd', '--state'],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0:
                firewall_info['firewalld']['status'] = result.stdout.strip()
                
                # Get active zones with details
                result = subprocess.run(
                    ['firewall-cmd', '--get-active-zones'],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if result.returncode == 0:
                    firewall_info['firewalld']['zones'] = result.stdout
                
                # Get all zones and their rules
                result = subprocess.run(
                    ['firewall-cmd', '--list-all-zones'],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if result.returncode == 0:
                    firewall_info['firewalld']['all_zones_detail'] = result.stdout
                
                # Get services
                result = subprocess.run(
                    ['firewall-cmd', '--list-services'],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if result.returncode == 0:
                    firewall_info['firewalld']['services'] = result.stdout.strip().split()
                
                # Get ports
                result = subprocess.run(
                    ['firewall-cmd', '--list-ports'],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if result.returncode == 0:
                    firewall_info['firewalld']['ports'] = result.stdout.strip().split()
                
                # Get rich rules
                result = subprocess.run(
                    ['firewall-cmd', '--list-rich-rules'],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if result.returncode == 0:
                    firewall_info['firewalld']['rich_rules'] = result.stdout.strip().split('\n')
        except:
            pass
        
        return firewall_info
    
    def _parse_iptables_output(self, output):
        """Parse iptables output into structured format"""
        rules = []
        lines = output.strip().split('\n')
        
        # Skip header lines
        data_start = False
        for line in lines:
            if line.startswith('num'):
                data_start = True
                continue
            
            if data_start and line.strip():
                parts = line.split()
                if len(parts) >= 3:
                    rule = {
                        'rule_number': parts[0] if parts[0].isdigit() else None,
                        'pkts': parts[1] if len(parts) > 1 else None,
                        'bytes': parts[2] if len(parts) > 2 else None,
                        'target': parts[3] if len(parts) > 3 else None,
                        'prot': parts[4] if len(parts) > 4 else None,
                        'opt': parts[5] if len(parts) > 5 else None,
                        'source': parts[6] if len(parts) > 6 else None,
                        'destination': parts[7] if len(parts) > 7 else None,
                        'extra': ' '.join(parts[8:]) if len(parts) > 8 else None,
                        'raw_line': line.strip()
                    }
                    rules.append(rule)
        
        return rules
    
    def _parse_ufw_rules(self, output):
        """Parse UFW rules output"""
        rules = []
        lines = output.strip().split('\n')
        
        in_rules_section = False
        for line in lines:
            # Detect default policies
            if 'Default:' in line:
                continue
            
            # Parse rule lines
            if 'ALLOW' in line or 'DENY' in line or 'REJECT' in line or 'LIMIT' in line:
                rules.append({
                    'raw_rule': line.strip(),
                    'action': 'ALLOW' if 'ALLOW' in line else 'DENY' if 'DENY' in line else 'REJECT' if 'REJECT' in line else 'LIMIT'
                })
        
        return rules
    
    def get_running_services(self):
        """Get running systemd services"""
        services = []
        
        try:
            result = subprocess.run(
                ['systemctl', 'list-units', '--type=service', '--state=running', '--no-pager'],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            for line in result.stdout.split('\n')[1:]:
                if '.service' in line:
                    parts = line.split()
                    if parts:
                        services.append({
                            'name': parts[0],
                            'load': parts[1] if len(parts) > 1 else None,
                            'active': parts[2] if len(parts) > 2 else None,
                            'sub': parts[3] if len(parts) > 3 else None
                        })
        except:
            pass
        
        return services
    
    def get_users(self):
        """Get system users"""
        users = []
        
        for user in psutil.users():
            users.append({
                'name': user.name,
                'terminal': user.terminal,
                'host': user.host,
                'started': datetime.datetime.fromtimestamp(user.started).isoformat(),
                'pid': user.pid if hasattr(user, 'pid') else None
            })
        
        return users
    
    def get_running_processes(self, limit=50):
        """Get top running processes"""
        processes = []
        
        for proc in psutil.process_iter(['pid', 'name', 'username', 'memory_percent', 'cpu_percent']):
            try:
                processes.append(proc.info)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        
        # Sort by memory usage and limit
        processes.sort(key=lambda x: x.get('memory_percent', 0), reverse=True)
        return processes[:limit]
    
    def collect_all(self):
        """Collect all inventory information"""
        print("Collecting server inventory...")
        
        print("- System information")
        system = self.get_system_info()
        
        print("- OS information")
        os_info = self.get_os_info()
        
        print("- CPU information")
        cpu = self.get_cpu_info()
        
        print("- Checking and enabling sysstat service if needed")
        sysstat_status = self.check_and_install_sysstat()
        
        print("- Previous day CPU utilization")
        cpu_history = self.get_previous_day_cpu_utilization()
        
        print("- Memory information")
        memory = self.get_memory_info()
        
        print("- Disk information")
        disks = self.get_disk_info()
        
        print("- Network information")
        network = self.get_network_info()
        
        print("- Service dependencies")
        service_deps = self.get_service_dependencies()
        
        print("- Application communication mapping")
        app_comm = self.get_application_communication_map()
        
        print("- Port-level dependencies")
        port_deps = self.get_port_dependencies()
        
        print("- Docker information")
        docker = self.get_docker_info()
        
        print("- Firewall configuration")
        firewall = self.get_firewall_info()
        
        print("- Users")
        users = self.get_users()
        
        print("- Installed packages")
        packages = self.get_installed_packages()
        
        print("- Running services")
        services = self.get_running_services()
        
        print("- Top processes")
        processes = self.get_running_processes()
        
        self.inventory = {
            'timestamp': datetime.datetime.now().isoformat(),
            'system': system,
            'os': os_info,
            'cpu': cpu,
            'cpu_history_previous_day': cpu_history,
            'sysstat_service_status': sysstat_status,
            'memory': memory,
            'disks': disks,
            'network': network,
            'service_dependencies': service_deps,
            'application_communication': app_comm,
            'port_dependencies': port_deps,
            'docker': docker,
            'firewall': firewall,
            'users': users,
            'packages': packages,
            'services': services,
            'top_processes': processes
        }
        
        print("\nInventory collection complete!")
        return self.inventory
    
    def save_to_file(self, filename='server_inventory_enhanced.json'):
        """Save inventory to JSON file"""
        # Create parent directory if it doesn't exist
        output_path = Path(filename)
        if output_path.parent != Path('.'):
            output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(filename, 'w') as f:
            json.dump(self.inventory, f, indent=2)
        print(f"Inventory saved to: {filename}")
    
    
    def print_json(self):
        """Print inventory as JSON"""
        print(json.dumps(self.inventory, indent=2))
    
    def print_summary(self):
        """Print a human-readable summary of key findings"""
        print("\n" + "="*80)
        print("SERVER INVENTORY SUMMARY")
        print("="*80)
        
        # System info
        if 'system' in self.inventory:
            sys = self.inventory['system']
            print(f"\nHostname: {sys.get('hostname')}")
            print(f"Platform: {sys.get('platform')} {sys.get('platform_release')}")
        
        # CPU History
        if 'cpu_history_previous_day' in self.inventory:
            cpu_hist = self.inventory['cpu_history_previous_day']
            if cpu_hist.get('available'):
                print(f"\nPrevious Day CPU Utilization ({cpu_hist.get('date')}):")
                if cpu_hist.get('average'):
                    avg = cpu_hist['average']
                    print(f"  Average:")
                    print(f"    - User: {avg.get('user', 0):.2f}%")
                    print(f"    - System: {avg.get('system', 0):.2f}%")
                    print(f"    - IOWait: {avg.get('iowait', 0):.2f}%")
                    print(f"    - Idle: {avg.get('idle', 0):.2f}%")
                print(f"  Data points collected: {len(cpu_hist.get('hourly_data', []))}")
            else:
                print(f"\nPrevious Day CPU Utilization: {cpu_hist.get('error', 'Not available')}")
        
        # Services with dependencies
        if 'service_dependencies' in self.inventory:
            active_services = [s for s, info in self.inventory['service_dependencies'].items() 
                             if info.get('status') == 'active']
            print(f"\nActive Services: {len(active_services)}")
            
            # Show services with most dependencies
            services_with_deps = [(s, len(info.get('requires', [])) + len(info.get('wants', []))) 
                                 for s, info in self.inventory['service_dependencies'].items()
                                 if info.get('status') == 'active']
            services_with_deps.sort(key=lambda x: x[1], reverse=True)
            
            if services_with_deps[:5]:
                print("\nTop 5 Services by Dependency Count:")
                for svc, count in services_with_deps[:5]:
                    if count > 0:
                        print(f"  - {svc}: {count} dependencies")
        
        # Application communication
        if 'application_communication' in self.inventory:
            comm = self.inventory['application_communication']
            print(f"\nApplication Communication:")
            print(f"  - Processes with connections: {len(comm.get('process_to_service', {}))}")
            print(f"  - Services with clients: {len(comm.get('service_clients', {}))}")
            print(f"  - Total communication paths: {len(comm.get('communication_matrix', []))}")
            
            # Show most connected services
            if comm.get('service_clients'):
                print("\n  Top Services by Client Count:")
                service_clients = [(s, len(c)) for s, c in comm['service_clients'].items()]
                service_clients.sort(key=lambda x: x[1], reverse=True)
                for svc, count in service_clients[:5]:
                    print(f"    - {svc}: {count} clients")
        
        # Port dependencies
        if 'port_dependencies' in self.inventory:
            port_deps = self.inventory['port_dependencies']
            listening = port_deps.get('listening_services', {})
            print(f"\nPort-Level Dependencies:")
            print(f"  - Listening ports: {len(listening)}")
            
            if listening:
                print("\n  Key Listening Services:")
                for port in sorted(listening.keys())[:10]:
                    info = listening[port]
                    clients = len(info.get('clients', []))
                    print(f"    - Port {port}: {info.get('service')} ({info.get('well_known_service')}) - {clients} clients")
        
        # Docker
        if 'docker' in self.inventory and self.inventory['docker'].get('installed'):
            docker = self.inventory['docker']
            print(f"\nDocker:")
            print(f"  - Containers running: {len(docker.get('containers', []))}")
            print(f"  - Networks: {len(docker.get('networks', []))}")
        
        print("\n" + "="*80)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Enhanced Server Inventory Discovery Tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This tool collects comprehensive server information including:
  - Service dependencies (systemd Requires, Wants, After, Before)
  - Application-to-application communication mapping
  - Port-level dependencies and service-to-port relationships
  - Network connections and firewall rules
  - Docker container information
  - Previous day CPU utilization from sysstat (sar)
  - Automatic systemd timer setup for scheduled collection (every 6 hours)
  
Example usage:
  sudo server-inventory -o inventory.json
  sudo server-inventory --summary
  sudo server-inventory --setup-systemd-only
        """
    )
    parser.add_argument(
        '-o', '--output',
        help='Output file path (default: print to stdout)',
        default=None
    )
    parser.add_argument(
        '-s', '--summary',
        action='store_true',
        help='Print human-readable summary instead of JSON'
    )
    parser.add_argument(
        '-p', '--pretty',
        action='store_true',
        help='Pretty print JSON output'
    )
    parser.add_argument(
        '--setup-systemd-only',
        action='store_true',
        help='Only setup systemd service and timer, do not collect inventory'
    )
    parser.add_argument(
        '--skip-systemd-setup',
        action='store_true',
        help='Skip automatic systemd service and timer setup'
    )
    
    args = parser.parse_args()
    
    # Check if running as root
    if os.geteuid() != 0:
        print("Warning: Running without root privileges. Some information may be incomplete.")
        print("Run with sudo for complete inventory.\n")
    
    inventory = ServerInventory()
    
    # If only setting up systemd, do that and exit
    if args.setup_systemd_only:
        if os.geteuid() != 0:
            print("Error: --setup-systemd-only requires root privileges")
            print("Please run with sudo")
            return 1
        
        print("Setting up systemd service and timer...")
        print("=" * 60)
        result = inventory.setup_systemd_service_and_timer()
        return 0 if not result.get('error') else 1
    
    # Setup systemd service and timer automatically (unless skipped)
    if not args.skip_systemd_setup and os.geteuid() == 0:
        print("\nSetting up systemd service and timer for automated collection...")
        print("=" * 60)
        inventory.setup_systemd_service_and_timer()
        print()
    
    # Collect inventory
    inventory.collect_all()
    
    if args.summary:
        inventory.print_summary()
    elif args.output:
        inventory.save_to_file(args.output)
        if args.summary:
            inventory.print_summary()
    else:
        inventory.print_json()
    
    return 0


if __name__ == '__main__':
    exit(main())
