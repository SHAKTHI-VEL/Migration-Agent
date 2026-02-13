#!/usr/bin/env python3
"""
Enhanced Windows Server Inventory Discovery Tool
Collects comprehensive server information including:
- Service dependencies
- Application-to-application communication
- Port-level dependencies
- Previous day CPU utilization from Performance Monitor
- Automatic Performance Monitor configuration via CLI
- Automatic scheduling via Windows Task Scheduler

FIXED: Now properly reads CSV files created by Performance Monitor
"""

import json
import platform
import socket
import subprocess
import os
import sys
import datetime
import psutil
import re
import shutil
from pathlib import Path
from collections import defaultdict
import ctypes
import winreg

# Detect if running as compiled executable without console
IS_COMPILED = getattr(sys, 'frozen', False)
NO_CONSOLE = IS_COMPILED and sys.stderr is None

# If running without console, redirect all output to log file
if NO_CONSOLE:
    log_dir = os.path.join(os.path.expanduser('~'), 'AppData', 'Local', 'WindowsServerInventory')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f'inventory_log_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.txt')
    
    # Redirect stdout and stderr to log file
    sys.stdout = open(log_file, 'w', encoding='utf-8')
    sys.stderr = sys.stdout

# Task Scheduler Configuration
TASK_NAME = "SystemInfoCollector"
INTERVAL_HOURS = 6


def get_exe_path():
    """
    Returns the absolute path of the running EXE or script
    """
    if getattr(sys, 'frozen', False):
        # Running as compiled executable
        return os.path.abspath(sys.executable)
    else:
        # Running as script
        return os.path.abspath(sys.argv[0])


def task_exists():
    """
    Check if the scheduled task already exists
    """
    result = subprocess.run(
        ["schtasks", "/Query", "/TN", TASK_NAME],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    return result.returncode == 0


def create_task():
    """
    Create a Windows scheduled task to run this script every INTERVAL_HOURS hours
    """
    exe_path = get_exe_path()
    
    # Set default output file for scheduled task
    output_file = os.path.join(
        os.path.expanduser('~'),
        'AppData',
        'Local',
        'WindowsServerInventory',
        'inventory.json'
    )
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Build command with output file
    command = f'"{exe_path}" -o "{output_file}"'
    
    try:
        subprocess.run(
            [
                "schtasks",
                "/Create",
                "/F",  # Force creation (overwrite if exists)
                "/SC", "HOURLY",
                "/MO", str(INTERVAL_HOURS),
                "/TN", TASK_NAME,
                "/TR", command,
                "/RL", "HIGHEST",  # Run with highest privileges
                "/RU", "SYSTEM"    # Run as SYSTEM account for true background execution
            ],
            check=True,
            capture_output=True,
            text=True
        )
        print(f"✓ Successfully created scheduled task '{TASK_NAME}'")
        print(f"  Task will run every {INTERVAL_HOURS} hours")
        print(f"  Executable: {exe_path}")
        print(f"  Output: {output_file}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed to create scheduled task: {e}")
        if e.stderr:
            print(f"  Error output: {e.stderr}")
        return False


def delete_task():
    """
    Delete the scheduled task
    """
    try:
        subprocess.run(
            ["schtasks", "/Delete", "/TN", TASK_NAME, "/F"],
            check=True,
            capture_output=True,
            text=True
        )
        print(f"✓ Successfully deleted scheduled task '{TASK_NAME}'")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Failed to delete scheduled task: {e}")
        return False


def get_task_info():
    """
    Get information about the scheduled task
    """
    try:
        result = subprocess.run(
            ["schtasks", "/Query", "/TN", TASK_NAME, "/FO", "LIST", "/V"],
            capture_output=True,
            text=True,
            check=True,
            encoding='utf-8',
            errors='ignore'
        )
        return result.stdout
    except subprocess.CalledProcessError:
        return None


def auto_setup_scheduled_task():
    """
    Automatically check and install scheduled task if not present
    This runs on EVERY execution of the script
    """
    try:
        # Only attempt if running as admin
        if not ctypes.windll.shell32.IsUserAnAdmin():
            print("\n[Auto-Setup] Scheduled task check skipped (requires admin privileges)")
            return False
        
        # Check if task exists
        if task_exists():
            print("\n[Auto-Setup] ✓ Scheduled task already exists")
            return True
        
        # Task doesn't exist, create it
        print("\n[Auto-Setup] Scheduled task not found, installing...")
        sys.stdout.flush()
        
        if create_task():
            print("[Auto-Setup] ✓ Scheduled task installed successfully!")
            sys.stdout.flush()
            return True
        else:
            print("[Auto-Setup] ✗ Failed to install scheduled task")
            sys.stdout.flush()
            return False
            
    except Exception as e:
        print(f"[Auto-Setup] Error during scheduled task setup: {str(e)}")
        sys.stdout.flush()
        return False


class WindowsServerInventory:
    def __init__(self):
        self.inventory = {}
        self.service_port_map = {}  # Maps services to their ports
        self.port_service_map = {}  # Maps ports to services
        self.process_connections = defaultdict(list)  # Maps processes to their connections
    
    def is_admin(self):
        """Check if script is running with administrator privileges"""
        try:
            return ctypes.windll.shell32.IsUserAnAdmin()
        except:
            return False
    
    def setup_performance_monitor(self):
        """Setup Performance Monitor data collector set via CLI with proper CSV format and counters"""
        result = {
            'success': False,
            'message': '',
            'collector_name': 'SystemInventoryCPU',
            'commands_executed': []
        }
        
        collector_name = result['collector_name']
        
        try:
            # Check if collector already exists
            check_cmd = ['logman', 'query', collector_name]
            result['commands_executed'].append(' '.join(check_cmd))
            check_result = subprocess.run(
                check_cmd,
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='ignore'
            )
            
            # Delete existing collector if it exists
            if check_result.returncode == 0:
                print(f"  Found existing collector '{collector_name}', deleting...")
                delete_cmd = ['logman', 'delete', collector_name]
                result['commands_executed'].append(' '.join(delete_cmd))
                subprocess.run(
                    delete_cmd,
                    capture_output=True,
                    text=True,
                    encoding='utf-8',
                    errors='ignore'
                )
            
            # Ensure the PerfLogs directory exists
            perf_log_base = 'C:\\PerfLogs\\Admin'
            perf_log_dir = f'{perf_log_base}\\{collector_name}'
            
            try:
                if not os.path.exists(perf_log_base):
                    os.makedirs(perf_log_base, exist_ok=True)
                    print(f"  Created directory: {perf_log_base}")
                
                if not os.path.exists(perf_log_dir):
                    os.makedirs(perf_log_dir, exist_ok=True)
                    print(f"  Created directory: {perf_log_dir}")
            except Exception as dir_error:
                print(f"  Warning: Could not create directory: {dir_error}")
            
            # Clean old CSV files to ensure fresh start
            print(f"  Cleaning old CSV files...")
            if os.path.exists(perf_log_dir):
                try:
                    for file in os.listdir(perf_log_dir):
                        if file.endswith('.csv') or file.endswith('.blg'):
                            file_path = os.path.join(perf_log_dir, file)
                            try:
                                os.remove(file_path)
                                print(f"    Deleted: {file}")
                            except Exception as e:
                                print(f"    Could not delete {file}: {e}")
                except Exception as e:
                    print(f"  Warning: Could not clean directory: {e}")
            
            # Create the data collector set with PROPER counter format
            print(f"  Creating Performance Monitor data collector '{collector_name}'...")
            
            # CRITICAL FIX: Use individual -c arguments for each counter
            # This is the correct way to add multiple counters that creates proper CSV columns
            create_cmd = [
                'logman', 'create', 'counter', collector_name,
                '-f', 'csv',
                '-v', 'mmddhhmm',
                '-si', '00:05:00',
                '-o', f'C:\\PerfLogs\\Admin\\{collector_name}\\cpu_metrics',
                '-c',
                r'\Processor(_Total)\% Processor Time',
                r'\Memory\Available MBytes',
                r'\PhysicalDisk(_Total)\% Disk Time'
            ]

            
            result['commands_executed'].append(' '.join(create_cmd))
            
            create_result = subprocess.run(
                create_cmd,
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='ignore'
            )
            
            if create_result.returncode != 0:
                error_detail = create_result.stderr if create_result.stderr else create_result.stdout
                print(f"  Multi-counter creation failed: {error_detail.strip()}")
                print(f"  Attempting single-counter configuration...")
                
                # Try simpler command with just CPU counter
                simple_cmd = [
                    'logman', 'create', 'counter', collector_name,
                    '-f', 'csv',
                    '-v', 'mmddhhmm',
                    '-si', '00:05:00',
                    '-o', f'C:\\PerfLogs\\Admin\\{collector_name}\\cpu_metrics',
                    '-c', r'\Processor(_Total)\% Processor Time'
                ]
                result['commands_executed'].append(' '.join(simple_cmd))
                
                simple_result = subprocess.run(
                    simple_cmd,
                    capture_output=True,
                    text=True,
                    encoding='utf-8',
                    errors='ignore'
                )
                
                if simple_result.returncode != 0:
                    error_detail = simple_result.stderr if simple_result.stderr else simple_result.stdout
                    result['message'] = f"Failed to create collector: {error_detail.strip()}"
                    result['return_code'] = simple_result.returncode
                    print(f"  Failed: {error_detail.strip()}")
                    return result
                
                print(f"  ✓ Created with CPU counter")
                
                # Now add additional counters
                print(f"  Adding memory and disk counters...")
                
                update_mem_cmd = ['logman', 'update', 'counter', collector_name, '-c', r'\Memory\Available MBytes']
                result['commands_executed'].append(' '.join(update_mem_cmd))
                subprocess.run(
                    update_mem_cmd,
                    capture_output=True,
                    text=True,
                    encoding='utf-8',
                    errors='ignore'
                )
                
                update_disk_cmd = ['logman', 'update', 'counter', collector_name, '-c', r'\PhysicalDisk(_Total)\% Disk Time']
                result['commands_executed'].append(' '.join(update_disk_cmd))
                subprocess.run(
                    update_disk_cmd,
                    capture_output=True,
                    text=True,
                    encoding='utf-8',
                    errors='ignore'
                )
                
                print(f"  ✓ Additional counters added")
            else:
                print(f"  ✓ Created data collector set with all counters")
            
            # Start the data collector
            print(f"  Starting data collector...")
            start_cmd = ['logman', 'start', collector_name]
            result['commands_executed'].append(' '.join(start_cmd))
            
            start_result = subprocess.run(
                start_cmd,
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='ignore'
            )
            
            if start_result.returncode != 0:
                result['message'] = f"Failed to start collector: {start_result.stderr}"
                return result
            
            print(f"  ✓ Started data collector")
            
            result['success'] = True
            result['message'] = (
                f"Performance Monitor data collector '{collector_name}' has been successfully configured and started.\n"
                f"  - Collecting CPU, Memory, and Disk metrics every 5 minutes\n"
                f"  - Logs saved to: C:\\PerfLogs\\Admin\\{collector_name}\\ (CSV format)\n"
                f"  - Wait 5-10 minutes for first data collection\n"
                f"  - Old CSV files have been cleaned to ensure fresh start\n"
                f"  - Each counter has its own column in the CSV file\n"
                f"  - Run inventory script again after 10 minutes to see CPU history data"
            )
            
        except Exception as e:
            result['message'] = f"Error setting up Performance Monitor: {str(e)}"
        
        return result
    
    def check_performance_monitor_status(self):
        """Check if Performance Monitor data collector is running"""
        collector_name = 'SystemInventoryCPU'
        
        try:
            result = subprocess.run(
                ['logman', 'query', collector_name],
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='ignore'
            )
            
            if result.returncode == 0:
                # Parse the output to check if it's running
                if 'Running' in result.stdout:
                    return {'exists': True, 'running': True, 'output': result.stdout}
                else:
                    return {'exists': True, 'running': False, 'output': result.stdout}
            else:
                return {'exists': False, 'running': False, 'output': None}
        except Exception as e:
            return {'exists': False, 'running': False, 'error': str(e)}
    
    def stop_performance_monitor(self):
        """Stop the Performance Monitor data collector"""
        collector_name = 'SystemInventoryCPU'
        
        try:
            result = subprocess.run(
                ['logman', 'stop', collector_name],
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='ignore'
            )
            
            if result.returncode == 0:
                return {'success': True, 'message': f"Stopped data collector '{collector_name}'"}
            else:
                return {'success': False, 'message': f"Failed to stop: {result.stderr}"}
        except Exception as e:
            return {'success': False, 'message': f"Error: {str(e)}"}
    
    def delete_performance_monitor(self):
        """Delete the Performance Monitor data collector"""
        collector_name = 'SystemInventoryCPU'
        
        try:
            # Stop it first
            subprocess.run(
                ['logman', 'stop', collector_name],
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='ignore'
            )
            
            # Delete it
            result = subprocess.run(
                ['logman', 'delete', collector_name],
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='ignore'
            )
            
            if result.returncode == 0:
                return {'success': True, 'message': f"Deleted data collector '{collector_name}'"}
            else:
                return {'success': False, 'message': f"Failed to delete: {result.stderr}"}
        except Exception as e:
            return {'success': False, 'message': f"Error: {str(e)}"}
    
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
        
        try:
            # Get Windows version info from registry
            key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, 
                                r'SOFTWARE\Microsoft\Windows NT\CurrentVersion')
            
            try:
                os_info['product_name'] = winreg.QueryValueEx(key, 'ProductName')[0]
            except:
                pass
            
            try:
                os_info['edition_id'] = winreg.QueryValueEx(key, 'EditionID')[0]
            except:
                pass
            
            try:
                os_info['release_id'] = winreg.QueryValueEx(key, 'ReleaseId')[0]
            except:
                pass
            
            try:
                os_info['current_build'] = winreg.QueryValueEx(key, 'CurrentBuild')[0]
            except:
                pass
            
            try:
                os_info['install_date'] = winreg.QueryValueEx(key, 'InstallDate')[0]
            except:
                pass
            
            winreg.CloseKey(key)
        except Exception as e:
            os_info['error'] = f'Error reading registry: {str(e)}'
        
        # Get system info using systeminfo command
        try:
            result = subprocess.run(
                ['systeminfo'],
                capture_output=True,
                text=True,
                timeout=30,
                encoding='utf-8',
                errors='ignore'
            )
            
            if result.returncode == 0:
                for line in result.stdout.split('\n'):
                    if ':' in line:
                        key, value = line.split(':', 1)
                        key = key.strip().lower().replace(' ', '_')
                        value = value.strip()
                        if value and key not in os_info:
                            os_info[key] = value
        except Exception as e:
            os_info['systeminfo_error'] = str(e)
        
        # Get uptime using WMI
        try:
            result = subprocess.run(
                ['wmic', 'os', 'get', 'lastbootuptime'],
                capture_output=True,
                text=True,
                timeout=10,
                encoding='utf-8',
                errors='ignore'
            )
            
            if result.returncode == 0:
                lines = result.stdout.strip().split('\n')
                if len(lines) > 1:
                    boot_time_str = lines[1].split('.')[0]
                    boot_time = datetime.datetime.strptime(boot_time_str, '%Y%m%d%H%M%S')
                    uptime_seconds = (datetime.datetime.now() - boot_time).total_seconds()
                    os_info['uptime_seconds'] = uptime_seconds
                    os_info['uptime_days'] = uptime_seconds / 86400
                    os_info['last_boot_time'] = boot_time.isoformat()
        except Exception as e:
            os_info['uptime_error'] = str(e)
        
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
        
        # Get CPU name from WMI
        try:
            result = subprocess.run(
                ['wmic', 'cpu', 'get', 'name'],
                capture_output=True,
                text=True,
                timeout=10,
                encoding='utf-8',
                errors='ignore'
            )
            
            if result.returncode == 0:
                lines = result.stdout.strip().split('\n')
                if len(lines) > 1:
                    cpu_info['model_name'] = lines[1].strip()
        except Exception as e:
            cpu_info['model_name_error'] = str(e)
        
        return cpu_info
    
    def get_previous_day_cpu_utilization(self):
        """Get previous day CPU utilization from Performance Monitor logs (CSV format)"""
        cpu_history = {
            'available': False,
            'date': None,
            'average': {},
            'hourly_data': [],
            'raw_output': None,
            'error': None
        }
        
        try:
            # Try to get CPU data from Performance Monitor
            yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
            cpu_history['date'] = yesterday.strftime('%Y-%m-%d')
            
            collector_name = 'SystemInventoryCPU'
            
            # Check if our collector exists and is collecting data
            perf_log_dir = f'C:\\PerfLogs\\Admin\\{collector_name}'
            
            if os.path.exists(perf_log_dir):
                # Look for Performance Monitor logs (both .blg binary and .csv text files)
                log_files = []
                csv_files = []
                
                for root, dirs, files in os.walk(perf_log_dir):
                    for file in files:
                        file_path = os.path.join(root, file)
                        # Check if file has recent data
                        try:
                            file_time = datetime.datetime.fromtimestamp(os.path.getmtime(file_path))
                            # Check files from last 2 days to capture yesterday's data
                            if (datetime.datetime.now() - file_time).days <= 2:
                                if file.endswith('.blg'):
                                    log_files.append(file_path)
                                elif file.endswith('.csv'):
                                    csv_files.append(file_path)
                        except:
                            pass
                
                # CHANGED: Prioritize CSV files since they're easier to parse
                files_to_process = csv_files if csv_files else log_files
                
                if files_to_process:
                    cpu_history['log_files_found'] = files_to_process
                    cpu_history['file_type'] = 'csv' if csv_files else 'blg'
                    
                    # Process CSV files directly
                    if csv_files:
                        print(f"  Found {len(csv_files)} CSV file(s)")
                        try:
                            import csv as csv_module
                            
                            cpu_values = []
                            hourly_data = []
                            
                            # Process all CSV files found
                            for csv_file in csv_files:
                                print(f"  Processing: {csv_file}")
                                
                                try:
                                    with open(csv_file, 'r', encoding='utf-8', errors='ignore') as f:
                                        reader = csv_module.reader(f)
                                        rows = list(reader)
                                        
                                        if len(rows) < 2:
                                            continue
                                        
                                        # First row contains headers
                                        headers = rows[0]
                                        
                                        # Find column indices
                                        timestamp_idx = None
                                        cpu_idx = None
                                        memory_idx = None
                                        disk_idx = None
                                        
                                        for i, header in enumerate(headers):
                                            if not timestamp_idx and ('Time' in header or 'PDH-CSV' in header):
                                                timestamp_idx = i
                                            if not cpu_idx and 'Processor Time' in header and '_Total' in header:
                                                cpu_idx = i
                                            if not memory_idx and 'Available MBytes' in header:
                                                memory_idx = i
                                            if not disk_idx and 'Disk Time' in header and '_Total' in header:
                                                disk_idx = i
                                        
                                        print(f"    Found columns - Timestamp: {timestamp_idx}, CPU: {cpu_idx}, Memory: {memory_idx}, Disk: {disk_idx}")
                                        
                                        # Process data rows
                                        for row in rows[1:]:
                                            if len(row) <= max(filter(None, [timestamp_idx, cpu_idx, memory_idx, disk_idx])):
                                                continue
                                            
                                            data_point = {}
                                            
                                            # Extract timestamp
                                            if timestamp_idx is not None and len(row) > timestamp_idx:
                                                data_point['timestamp'] = row[timestamp_idx].strip('"')
                                            
                                            # Extract CPU value
                                            if cpu_idx is not None and len(row) > cpu_idx:
                                                try:
                                                    cpu_val = float(row[cpu_idx].strip('"'))
                                                    cpu_values.append(cpu_val)
                                                    data_point['cpu_percent'] = round(cpu_val, 2)
                                                except (ValueError, TypeError):
                                                    pass
                                            
                                            # Extract memory value
                                            if memory_idx is not None and len(row) > memory_idx:
                                                try:
                                                    mem_val = float(row[memory_idx].strip('"'))
                                                    data_point['memory_available_mb'] = round(mem_val, 2)
                                                except (ValueError, TypeError):
                                                    pass
                                            
                                            # Extract disk value
                                            if disk_idx is not None and len(row) > disk_idx:
                                                try:
                                                    disk_val = float(row[disk_idx].strip('"'))
                                                    data_point['disk_percent'] = round(disk_val, 2)
                                                except (ValueError, TypeError):
                                                    pass
                                            
                                            if data_point and ('cpu_percent' in data_point or 'memory_available_mb' in data_point):
                                                hourly_data.append(data_point)
                                
                                except Exception as file_error:
                                    print(f"    Error processing {csv_file}: {file_error}")
                                    cpu_history['file_errors'] = cpu_history.get('file_errors', [])
                                    cpu_history['file_errors'].append(f"{csv_file}: {str(file_error)}")
                            
                            if cpu_values:
                                cpu_history['available'] = True
                                cpu_history['average'] = {
                                    'total': round(sum(cpu_values) / len(cpu_values), 2),
                                    'min': round(min(cpu_values), 2),
                                    'max': round(max(cpu_values), 2)
                                }
                                cpu_history['data_points'] = len(cpu_values)
                                cpu_history['hourly_data'] = hourly_data
                                cpu_history['sample_interval_minutes'] = 5
                                cpu_history['total_samples'] = len(hourly_data)
                                
                                print(f"  ✓ Successfully parsed {len(cpu_values)} CPU data points")
                            else:
                                cpu_history['error'] = 'CSV files found but no valid CPU data extracted'
                                cpu_history['note'] = 'Performance Monitor may have just started. Wait a few minutes for data collection, or check that the collector is running properly.'
                                cpu_history['debug_info'] = {
                                    'csv_files_processed': len(csv_files),
                                    'total_rows_found': len(hourly_data),
                                    'suggestion': 'Run diagnose_csv.py to inspect the CSV file contents'
                                }
                        
                        except Exception as e:
                            cpu_history['parse_error'] = str(e)
                            print(f"  Error parsing CSV files: {e}")
                    
                    # Process .blg files (binary format) - need relog conversion
                    elif log_files:
                        print(f"  Found {len(log_files)} binary log file(s), converting to CSV...")
                        try:
                            # Use the most recent log file
                            log_file = log_files[0]
                            
                            # Export to CSV for easier parsing
                            csv_output = f'C:\\PerfLogs\\temp_cpu_export_{datetime.datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
                            
                            relog_cmd = [
                                'relog', log_file,
                                '-f', 'CSV',
                                '-o', csv_output,
                                '-y'  # Answer yes to overwrite
                            ]
                            
                            relog_result = subprocess.run(
                                relog_cmd,
                                capture_output=True,
                                text=True,
                                timeout=60,
                                encoding='utf-8',
                                errors='ignore'
                            )
                            
                            if relog_result.returncode == 0 and os.path.exists(csv_output):
                                # Parse the CSV file (same logic as above)
                                try:
                                    import csv as csv_module
                                    with open(csv_output, 'r', encoding='utf-8', errors='ignore') as f:
                                        reader = csv_module.DictReader(f)
                                        rows = list(reader)
                                        
                                        if rows:
                                            cpu_values = []
                                            hourly_data = []
                                            timestamp_key = None
                                            cpu_key = None
                                            memory_key = None
                                            disk_key = None
                                            
                                            # Find the column names
                                            first_row = rows[0]
                                            for key in first_row.keys():
                                                if not timestamp_key and ('PDH-CSV' in key or 'Time' in key):
                                                    timestamp_key = key
                                                if not cpu_key and 'Processor Time' in key and '_Total' in key:
                                                    cpu_key = key
                                                if not memory_key and 'Available MBytes' in key:
                                                    memory_key = key
                                                if not disk_key and 'Disk Time' in key and '_Total' in key:
                                                    disk_key = key
                                            
                                            for row in rows:
                                                try:
                                                    data_point = {}
                                                    
                                                    if timestamp_key and row.get(timestamp_key):
                                                        data_point['timestamp'] = row[timestamp_key]
                                                    
                                                    if cpu_key and row.get(cpu_key):
                                                        try:
                                                            cpu_val = float(row[cpu_key])
                                                            cpu_values.append(cpu_val)
                                                            data_point['cpu_percent'] = round(cpu_val, 2)
                                                        except (ValueError, TypeError):
                                                            pass
                                                    
                                                    if memory_key and row.get(memory_key):
                                                        try:
                                                            mem_val = float(row[memory_key])
                                                            data_point['memory_available_mb'] = round(mem_val, 2)
                                                        except (ValueError, TypeError):
                                                            pass
                                                    
                                                    if disk_key and row.get(disk_key):
                                                        try:
                                                            disk_val = float(row[disk_key])
                                                            data_point['disk_percent'] = round(disk_val, 2)
                                                        except (ValueError, TypeError):
                                                            pass
                                                    
                                                    if data_point and ('cpu_percent' in data_point or 'memory_available_mb' in data_point):
                                                        hourly_data.append(data_point)
                                                        
                                                except (ValueError, KeyError) as e:
                                                    continue
                                            
                                            if cpu_values:
                                                cpu_history['available'] = True
                                                cpu_history['average'] = {
                                                    'total': round(sum(cpu_values) / len(cpu_values), 2),
                                                    'min': round(min(cpu_values), 2),
                                                    'max': round(max(cpu_values), 2)
                                                }
                                                cpu_history['data_points'] = len(cpu_values)
                                                cpu_history['hourly_data'] = hourly_data
                                                cpu_history['sample_interval_minutes'] = 5
                                                cpu_history['total_samples'] = len(hourly_data)
                                    
                                    # Clean up temp file
                                    os.remove(csv_output)
                                    
                                except Exception as e:
                                    cpu_history['parse_error'] = str(e)
                            else:
                                cpu_history['relog_error'] = relog_result.stderr
                        
                        except Exception as e:
                            cpu_history['extraction_error'] = str(e)
                    
                    if not cpu_history['available']:
                        cpu_history['note'] = 'Performance Monitor logs found but data extraction needs more time or configuration.'
                else:
                    cpu_history['error'] = f'No Performance Monitor logs (.csv or .blg) found in {perf_log_dir}'
                    cpu_history['note'] = 'Data collector may need to run for at least 24 hours to collect historical data.'
            else:
                # Check if data collector exists but isn't configured
                pm_status = self.check_performance_monitor_status()
                if not pm_status['exists']:
                    cpu_history['error'] = f'Performance Monitor data collector not configured'
                    cpu_history['note'] = 'Run with --enable-perfmon to automatically configure CPU data collection'
                else:
                    cpu_history['error'] = f'Performance Monitor log directory not found: {perf_log_dir}'
                    if pm_status['running']:
                        cpu_history['note'] = 'Data collector is running but needs time to collect data (wait 24 hours)'
                    else:
                        cpu_history['note'] = 'Data collector exists but is not running. Run with --enable-perfmon to start it.'
                
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
            'swap_total_bytes': swap.total,
            'swap_total_gb': round(swap.total / (1024**3), 2),
            'swap_used_bytes': swap.used,
            'swap_used_gb': round(swap.used / (1024**3), 2),
            'swap_percent': swap.percent
        }
    
    def get_disk_info(self):
        """Gather disk information"""
        disks = []
        
        for partition in psutil.disk_partitions():
            try:
                usage = psutil.disk_usage(partition.mountpoint)
                disk_info = {
                    'device': partition.device,
                    'mountpoint': partition.mountpoint,
                    'fstype': partition.fstype,
                    'opts': partition.opts,
                    'total_bytes': usage.total,
                    'total_gb': round(usage.total / (1024**3), 2),
                    'used_bytes': usage.used,
                    'used_gb': round(usage.used / (1024**3), 2),
                    'free_bytes': usage.free,
                    'free_gb': round(usage.free / (1024**3), 2),
                    'percent_used': usage.percent
                }
                disks.append(disk_info)
            except PermissionError:
                continue
        
        return disks
    
    def get_network_info(self):
        """Gather network information"""
        network_info = {
            'interfaces': [],
            'connections': [],
            'stats': {}
        }
        
        # Get network interfaces
        for iface, addrs in psutil.net_if_addrs().items():
            iface_info = {
                'name': iface,
                'addresses': []
            }
            
            for addr in addrs:
                addr_info = {
                    'family': str(addr.family),
                    'address': addr.address,
                    'netmask': addr.netmask,
                    'broadcast': addr.broadcast
                }
                iface_info['addresses'].append(addr_info)
            
            # Get interface stats
            try:
                stats = psutil.net_if_stats()[iface]
                iface_info['is_up'] = stats.isup
                iface_info['speed_mbps'] = stats.speed
                iface_info['mtu'] = stats.mtu
            except:
                pass
            
            # Get IO counters
            try:
                io = psutil.net_io_counters(pernic=True)[iface]
                iface_info['bytes_sent'] = io.bytes_sent
                iface_info['bytes_recv'] = io.bytes_recv
                iface_info['packets_sent'] = io.packets_sent
                iface_info['packets_recv'] = io.packets_recv
                iface_info['errin'] = io.errin
                iface_info['errout'] = io.errout
                iface_info['dropin'] = io.dropin
                iface_info['dropout'] = io.dropout
            except:
                pass
            
            network_info['interfaces'].append(iface_info)
        
        # Get network connections
        try:
            connections = psutil.net_connections(kind='inet')
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
                network_info['connections'].append(conn_info)
                
                # Map processes to connections
                if conn.pid:
                    self.process_connections[conn.pid].append(conn_info)
        except (PermissionError, psutil.AccessDenied):
            network_info['connections_error'] = 'Requires administrator privileges'
        
        # Get total network IO stats
        io_counters = psutil.net_io_counters()
        network_info['stats'] = {
            'bytes_sent': io_counters.bytes_sent,
            'bytes_recv': io_counters.bytes_recv,
            'packets_sent': io_counters.packets_sent,
            'packets_recv': io_counters.packets_recv,
            'errin': io_counters.errin,
            'errout': io_counters.errout,
            'dropin': io_counters.dropin,
            'dropout': io_counters.dropout
        }
        
        return network_info
    
    def get_service_dependencies(self):
        """Get Windows service dependencies"""
        services = {}
        
        try:
            # Get all services using sc query
            result = subprocess.run(
                ['sc', 'query', 'state=', 'all'],
                capture_output=True,
                text=True,
                timeout=30,
                encoding='utf-8',
                errors='ignore'
            )
            
            if result.returncode == 0:
                # Parse service names
                service_names = []
                for line in result.stdout.split('\n'):
                    if 'SERVICE_NAME:' in line:
                        service_name = line.split(':', 1)[1].strip()
                        service_names.append(service_name)
                
                # Get detailed info for each service
                for service_name in service_names:
                    try:
                        # Get service configuration
                        config_result = subprocess.run(
                            ['sc', 'qc', service_name],
                            capture_output=True,
                            text=True,
                            timeout=10,
                            encoding='utf-8',
                            errors='ignore'
                        )
                        
                        # Get service status
                        status_result = subprocess.run(
                            ['sc', 'query', service_name],
                            capture_output=True,
                            text=True,
                            timeout=10,
                            encoding='utf-8',
                            errors='ignore'
                        )
                        
                        service_info = {
                            'name': service_name,
                            'status': 'unknown',
                            'start_type': 'unknown',
                            'dependencies': [],
                            'display_name': '',
                            'binary_path': '',
                            'service_type': ''
                        }
                        
                        # Parse configuration
                        if config_result.returncode == 0:
                            for line in config_result.stdout.split('\n'):
                                line = line.strip()
                                if 'DISPLAY_NAME' in line:
                                    service_info['display_name'] = line.split(':', 1)[1].strip()
                                elif 'START_TYPE' in line:
                                    service_info['start_type'] = line.split(':', 1)[1].strip()
                                elif 'BINARY_PATH_NAME' in line:
                                    service_info['binary_path'] = line.split(':', 1)[1].strip()
                                elif 'SERVICE_TYPE' in line:
                                    service_info['service_type'] = line.split(':', 1)[1].strip()
                                elif 'DEPENDENCIES' in line:
                                    deps_line = line.split(':', 1)[1].strip()
                                    if deps_line:
                                        service_info['dependencies'] = [d.strip() for d in deps_line.split('\n') if d.strip()]
                        
                        # Parse status
                        if status_result.returncode == 0:
                            for line in status_result.stdout.split('\n'):
                                line = line.strip()
                                if 'STATE' in line:
                                    parts = line.split(':')
                                    if len(parts) > 1:
                                        state_info = parts[1].strip()
                                        if 'RUNNING' in state_info:
                                            service_info['status'] = 'running'
                                        elif 'STOPPED' in state_info:
                                            service_info['status'] = 'stopped'
                                        elif 'PAUSED' in state_info:
                                            service_info['status'] = 'paused'
                        
                        services[service_name] = service_info
                        
                    except Exception as e:
                        services[service_name] = {'error': str(e)}
                        
        except Exception as e:
            return {'error': f'Error getting service dependencies: {str(e)}'}
        
        return services
    
    def get_application_communication(self):
        """Map application-to-application communication"""
        communication = {
            'process_to_service': {},
            'service_clients': defaultdict(list),
            'communication_matrix': []
        }
        
        try:
            connections = psutil.net_connections(kind='inet')
            
            for conn in connections:
                if not conn.pid or not conn.raddr:
                    continue
                
                try:
                    proc = psutil.Process(conn.pid)
                    proc_name = proc.name()
                    proc_info = {
                        'pid': conn.pid,
                        'name': proc_name,
                        'exe': proc.exe() if proc.exe() else 'unknown',
                        'cmdline': ' '.join(proc.cmdline()[:3]) if proc.cmdline() else ''
                    }
                    
                    local_addr = f"{conn.laddr.ip}:{conn.laddr.port}" if conn.laddr else "unknown"
                    remote_addr = f"{conn.raddr.ip}:{conn.raddr.port}"
                    
                    # Try to identify remote service
                    remote_service = self.identify_service_by_port(conn.raddr.port)
                    
                    comm_entry = {
                        'source_process': proc_name,
                        'source_pid': conn.pid,
                        'source_address': local_addr,
                        'destination_address': remote_addr,
                        'destination_port': conn.raddr.port,
                        'destination_service': remote_service,
                        'protocol': str(conn.type),
                        'status': conn.status
                    }
                    
                    communication['communication_matrix'].append(comm_entry)
                    
                    # Track process to service mapping
                    if proc_name not in communication['process_to_service']:
                        communication['process_to_service'][proc_name] = {
                            'process_info': proc_info,
                            'connects_to': []
                        }
                    
                    communication['process_to_service'][proc_name]['connects_to'].append({
                        'service': remote_service,
                        'address': remote_addr,
                        'port': conn.raddr.port
                    })
                    
                    # Track service clients
                    if remote_service:
                        communication['service_clients'][remote_service].append({
                            'client_process': proc_name,
                            'client_pid': conn.pid,
                            'local_address': local_addr
                        })
                
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            
            # Convert defaultdict to regular dict for JSON serialization
            communication['service_clients'] = dict(communication['service_clients'])
            
        except (PermissionError, psutil.AccessDenied):
            communication['error'] = 'Requires administrator privileges'
        
        return communication
    
    def identify_service_by_port(self, port):
        """Identify well-known service by port number"""
        well_known_ports = {
            20: 'FTP-DATA', 21: 'FTP', 22: 'SSH', 23: 'Telnet',
            25: 'SMTP', 53: 'DNS', 80: 'HTTP', 110: 'POP3',
            143: 'IMAP', 443: 'HTTPS', 445: 'SMB', 3306: 'MySQL',
            3389: 'RDP', 5432: 'PostgreSQL', 5900: 'VNC',
            6379: 'Redis', 8080: 'HTTP-Alt', 8443: 'HTTPS-Alt',
            27017: 'MongoDB', 1433: 'MSSQL', 1521: 'Oracle'
        }
        return well_known_ports.get(port, f'Port-{port}')
    
    def get_port_dependencies(self):
        """Get detailed port-level dependencies"""
        port_deps = {
            'listening_services': {},
            'outbound_connections': {},
            'port_to_process': {}
        }
        
        try:
            connections = psutil.net_connections(kind='inet')
            
            for conn in connections:
                if not conn.laddr:
                    continue
                
                local_port = conn.laddr.port
                
                # Track listening services
                if conn.status == 'LISTEN':
                    if local_port not in port_deps['listening_services']:
                        port_deps['listening_services'][local_port] = {
                            'port': local_port,
                            'well_known_service': self.identify_service_by_port(local_port),
                            'processes': [],
                            'clients': []
                        }
                    
                    if conn.pid:
                        try:
                            proc = psutil.Process(conn.pid)
                            proc_info = {
                                'pid': conn.pid,
                                'name': proc.name(),
                                'exe': proc.exe() if proc.exe() else 'unknown'
                            }
                            
                            if proc_info not in port_deps['listening_services'][local_port]['processes']:
                                port_deps['listening_services'][local_port]['processes'].append(proc_info)
                            
                            # Map port to process
                            if local_port not in port_deps['port_to_process']:
                                port_deps['port_to_process'][local_port] = []
                            
                            if proc_info not in port_deps['port_to_process'][local_port]:
                                port_deps['port_to_process'][local_port].append(proc_info)
                        
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            pass
                
                # Track established connections to listening services
                elif conn.status == 'ESTABLISHED' and conn.raddr:
                    remote_port = conn.raddr.port
                    
                    if remote_port in port_deps['listening_services']:
                        client_info = {
                            'address': f"{conn.laddr.ip}:{conn.laddr.port}",
                            'pid': conn.pid
                        }
                        
                        if conn.pid:
                            try:
                                proc = psutil.Process(conn.pid)
                                client_info['process_name'] = proc.name()
                            except:
                                pass
                        
                        if client_info not in port_deps['listening_services'][remote_port]['clients']:
                            port_deps['listening_services'][remote_port]['clients'].append(client_info)
        
        except (PermissionError, psutil.AccessDenied):
            port_deps['error'] = 'Requires administrator privileges'
        
        return port_deps
    
    def get_docker_info(self):
        """Get Docker container information (if Docker is installed)"""
        docker_info = {
            'installed': False,
            'containers': [],
            'images': [],
            'networks': [],
            'volumes': []
        }
        
        # Check if Docker is installed
        if not shutil.which('docker'):
            docker_info['error'] = 'Docker not installed'
            return docker_info
        
        docker_info['installed'] = True
        
        try:
            # Get containers
            result = subprocess.run(
                ['docker', 'ps', '-a', '--format', '{{json .}}'],
                capture_output=True,
                text=True,
                timeout=30,
                encoding='utf-8',
                errors='ignore'
            )
            
            if result.returncode == 0:
                for line in result.stdout.strip().split('\n'):
                    if line:
                        try:
                            container = json.loads(line)
                            docker_info['containers'].append(container)
                        except:
                            pass
        
        except Exception as e:
            docker_info['containers_error'] = str(e)
        
        try:
            # Get images
            result = subprocess.run(
                ['docker', 'images', '--format', '{{json .}}'],
                capture_output=True,
                text=True,
                timeout=30,
                encoding='utf-8',
                errors='ignore'
            )
            
            if result.returncode == 0:
                for line in result.stdout.strip().split('\n'):
                    if line:
                        try:
                            image = json.loads(line)
                            docker_info['images'].append(image)
                        except:
                            pass
        
        except Exception as e:
            docker_info['images_error'] = str(e)
        
        try:
            # Get networks
            result = subprocess.run(
                ['docker', 'network', 'ls', '--format', '{{json .}}'],
                capture_output=True,
                text=True,
                timeout=30,
                encoding='utf-8',
                errors='ignore'
            )
            
            if result.returncode == 0:
                for line in result.stdout.strip().split('\n'):
                    if line:
                        try:
                            network = json.loads(line)
                            docker_info['networks'].append(network)
                        except:
                            pass
        
        except Exception as e:
            docker_info['networks_error'] = str(e)
        
        return docker_info
    
    def get_firewall_info(self):
        """Get Windows Firewall information"""
        firewall_info = {
            'profiles': {},
            'rules': []
        }
        
        try:
            # Get firewall state
            result = subprocess.run(
                ['netsh', 'advfirewall', 'show', 'allprofiles', 'state'],
                capture_output=True,
                text=True,
                timeout=30,
                encoding='utf-8',
                errors='ignore'
            )
            
            if result.returncode == 0:
                current_profile = None
                for line in result.stdout.split('\n'):
                    line = line.strip()
                    if 'Profile Settings' in line:
                        current_profile = line.split()[0].lower()
                        firewall_info['profiles'][current_profile] = {}
                    elif 'State' in line and current_profile:
                        state = line.split()[-1]
                        firewall_info['profiles'][current_profile]['state'] = state
        
        except Exception as e:
            firewall_info['state_error'] = str(e)
        
        try:
            # Get firewall rules (top 100 for performance)
            result = subprocess.run(
                ['netsh', 'advfirewall', 'firewall', 'show', 'rule', 'name=all'],
                capture_output=True,
                text=True,
                timeout=30,
                encoding='utf-8',
                errors='ignore'
            )
            
            if result.returncode == 0:
                rule = {}
                rule_count = 0
                max_rules = 100  # Limit for performance
                
                for line in result.stdout.split('\n'):
                    line = line.strip()
                    if not line:
                        if rule and rule_count < max_rules:
                            firewall_info['rules'].append(rule)
                            rule_count += 1
                        rule = {}
                    elif ':' in line:
                        key, value = line.split(':', 1)
                        rule[key.strip().lower().replace(' ', '_')] = value.strip()
                    
                    if rule_count >= max_rules:
                        break
                
                if rule_count >= max_rules:
                    firewall_info['rules_note'] = f'Showing first {max_rules} rules only'
        
        except Exception as e:
            firewall_info['rules_error'] = str(e)
        
        return firewall_info
    
    def get_users(self):
        """Get user information"""
        users = []
        
        try:
            # Get local users
            result = subprocess.run(
                ['net', 'user'],
                capture_output=True,
                text=True,
                timeout=30,
                encoding='utf-8',
                errors='ignore'
            )
            
            if result.returncode == 0:
                lines = result.stdout.split('\n')
                user_section = False
                
                for line in lines:
                    line = line.strip()
                    if '---' in line:
                        user_section = True
                        continue
                    
                    if user_section and line:
                        # Parse usernames (they can be in columns)
                        usernames = line.split()
                        for username in usernames:
                            if username and not username.startswith('The command'):
                                users.append({'username': username})
        
        except Exception as e:
            return {'error': str(e)}
        
        return users
    
    def get_installed_packages(self):
        """Get installed programs"""
        packages = []
        
        try:
            # Get installed programs from registry
            reg_paths = [
                r'SOFTWARE\Microsoft\Windows\CurrentVersion\Uninstall',
                r'SOFTWARE\WOW6432Node\Microsoft\Windows\CurrentVersion\Uninstall'
            ]
            
            for reg_path in reg_paths:
                try:
                    key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, reg_path)
                    
                    i = 0
                    while True:
                        try:
                            subkey_name = winreg.EnumKey(key, i)
                            subkey = winreg.OpenKey(key, subkey_name)
                            
                            try:
                                display_name = winreg.QueryValueEx(subkey, 'DisplayName')[0]
                                
                                package_info = {
                                    'name': display_name,
                                    'registry_key': subkey_name
                                }
                                
                                try:
                                    package_info['version'] = winreg.QueryValueEx(subkey, 'DisplayVersion')[0]
                                except:
                                    pass
                                
                                try:
                                    package_info['publisher'] = winreg.QueryValueEx(subkey, 'Publisher')[0]
                                except:
                                    pass
                                
                                try:
                                    package_info['install_date'] = winreg.QueryValueEx(subkey, 'InstallDate')[0]
                                except:
                                    pass
                                
                                packages.append(package_info)
                            
                            except (FileNotFoundError, OSError):
                                pass
                            
                            winreg.CloseKey(subkey)
                            i += 1
                        
                        except OSError:
                            break
                    
                    winreg.CloseKey(key)
                
                except Exception:
                    continue
        
        except Exception as e:
            return {'error': str(e)}
        
        return packages
    
    def get_running_services(self):
        """Get running Windows services"""
        services = []
        
        try:
            result = subprocess.run(
                ['sc', 'query', 'state=', 'all'],
                capture_output=True,
                text=True,
                timeout=30,
                encoding='utf-8',
                errors='ignore'
            )
            
            if result.returncode == 0:
                service = {}
                
                for line in result.stdout.split('\n'):
                    line = line.strip()
                    
                    if 'SERVICE_NAME:' in line:
                        if service:
                            services.append(service)
                        service = {'name': line.split(':', 1)[1].strip()}
                    
                    elif 'DISPLAY_NAME:' in line:
                        service['display_name'] = line.split(':', 1)[1].strip()
                    
                    elif 'STATE' in line:
                        parts = line.split(':')
                        if len(parts) > 1:
                            state_info = parts[1].strip()
                            if 'RUNNING' in state_info:
                                service['state'] = 'running'
                            elif 'STOPPED' in state_info:
                                service['state'] = 'stopped'
                            elif 'PAUSED' in state_info:
                                service['state'] = 'paused'
                
                if service:
                    services.append(service)
        
        except Exception as e:
            return {'error': str(e)}
        
        return services
    
    def get_running_processes(self, top_n=20):
        """Get information about running processes"""
        processes = []
        
        for proc in psutil.process_iter(['pid', 'name', 'username', 'cpu_percent', 'memory_percent', 'status']):
            try:
                pinfo = proc.info
                pinfo['cpu_percent'] = proc.cpu_percent(interval=0.1)
                processes.append(pinfo)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        
        # Sort by CPU usage and return top N
        processes.sort(key=lambda x: x.get('cpu_percent', 0), reverse=True)
        return processes[:top_n]
    
    def collect_all(self):
        """Collect all inventory information"""
        print("Starting Windows Server Inventory Collection...")
        sys.stdout.flush()
        print(f"Running as administrator: {self.is_admin()}")
        sys.stdout.flush()
        
        if not self.is_admin():
            print("\nWARNING: Not running as administrator.")
            print("Some information may be incomplete. Run as administrator for full inventory.\n")
            sys.stdout.flush()
        
        # Automatically enable Performance Monitor if not already running
        print("\n- Checking Performance Monitor status...")
        sys.stdout.flush()
        pm_status = self.check_performance_monitor_status()
        
        if not pm_status['exists']:
            print("  Performance Monitor data collector not configured")
            if self.is_admin():
                print("  Attempting to enable Performance Monitor automatically...")
                try:
                    result = self.setup_performance_monitor()
                    if result['success']:
                        print("  ✓ Performance Monitor enabled successfully")
                        print("    Data collection will begin immediately")
                        print("    Historical data will be available after 24 hours")
                    else:
                        print(f"  ✗ Failed to enable Performance Monitor: {result['message']}")
                        print("    Run with --enable-perfmon flag manually if needed")
                except Exception as e:
                    print(f"  ✗ Error enabling Performance Monitor: {str(e)}")
            else:
                print("  Note: Administrator privileges required to enable Performance Monitor")
                print("        Run with --enable-perfmon as Administrator to configure")
        elif not pm_status['running']:
            print("  Performance Monitor data collector exists but is not running")
            if self.is_admin():
                print("  Attempting to start Performance Monitor...")
                try:
                    start_cmd = ['logman', 'start', 'SystemInventoryCPU']
                    start_result = subprocess.run(
                        start_cmd,
                        capture_output=True,
                        text=True,
                        encoding='utf-8',
                        errors='ignore'
                    )
                    if start_result.returncode == 0:
                        print("  ✓ Performance Monitor started successfully")
                    else:
                        print(f"  ✗ Failed to start: {start_result.stderr}")
                        print("    Run with --enable-perfmon to reconfigure")
                except Exception as e:
                    print(f"  ✗ Error starting Performance Monitor: {str(e)}")
            else:
                print("  Note: Administrator privileges required to start Performance Monitor")
        else:
            print("  ✓ Performance Monitor is running and collecting data")
        
        print("\nCollecting:")
        print("- System information")
        system = self.get_system_info()
        
        print("- OS information")
        os_info = self.get_os_info()
        
        print("- CPU information")
        cpu = self.get_cpu_info()
        
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
        
        print("- Application communication")
        app_comm = self.get_application_communication()
        
        print("- Port dependencies")
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
        
        # Get final Performance Monitor status after auto-enable attempt
        pm_status_final = self.check_performance_monitor_status()
        
        self.inventory = {
            'timestamp': datetime.datetime.now().isoformat(),
            'system': system,
            'os': os_info,
            'cpu': cpu,
            'cpu_history_previous_day': cpu_history,
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
            'top_processes': processes,
            'scheduled_task_info': {
                'task_name': TASK_NAME,
                'interval_hours': INTERVAL_HOURS,
                'task_exists': task_exists()
            },
            'performance_monitor_status': pm_status_final
        }
        
        print("\nInventory collection complete!")
        return self.inventory
    
    def save_to_file(self, filename='windows_server_inventory_enhanced.json'):
        """Save inventory to JSON file"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.inventory, f, indent=2, ensure_ascii=False)
        print(f"Inventory saved to: {filename}")
    
    def print_json(self):
        """Print inventory as JSON"""
        print(json.dumps(self.inventory, indent=2, ensure_ascii=False))
    
    def print_summary(self):
        """Print a human-readable summary of key findings"""
        print("\n" + "="*80)
        print("WINDOWS SERVER INVENTORY SUMMARY")
        print("="*80)
        
        # System info
        if 'system' in self.inventory:
            sys_info = self.inventory['system']
            print(f"\nHostname: {sys_info.get('hostname')}")
            print(f"Platform: {sys_info.get('platform')} {sys_info.get('platform_release')}")
        
        # OS info
        if 'os' in self.inventory:
            os_info = self.inventory['os']
            if 'product_name' in os_info:
                print(f"OS: {os_info.get('product_name')}")
            if 'uptime_days' in os_info:
                print(f"Uptime: {os_info.get('uptime_days', 0):.2f} days")
        
        # Scheduled task info
        if 'scheduled_task_info' in self.inventory:
            task_info = self.inventory['scheduled_task_info']
            print(f"\nScheduled Task:")
            print(f"  - Name: {task_info.get('task_name')}")
            print(f"  - Interval: Every {task_info.get('interval_hours')} hours")
            print(f"  - Status: {'Registered' if task_info.get('task_exists') else 'Not registered'}")
        
        # Performance Monitor status
        if 'performance_monitor_status' in self.inventory:
            pm_status = self.inventory['performance_monitor_status']
            print(f"\nPerformance Monitor:")
            if pm_status.get('exists'):
                status = 'Running' if pm_status.get('running') else 'Stopped'
                print(f"  - Status: {status}")
                print(f"  - Collector: SystemInventoryCPU")
            else:
                print(f"  - Status: Not configured")
                print(f"  - Run with --enable-perfmon to configure")
        
        # CPU History
        if 'cpu_history_previous_day' in self.inventory:
            cpu_hist = self.inventory['cpu_history_previous_day']
            if cpu_hist.get('available'):
                print(f"\nPrevious Day CPU Utilization ({cpu_hist.get('date')}):")
                if cpu_hist.get('average'):
                    avg = cpu_hist['average']
                    print(f"  Average: {avg.get('total', 0):.2f}%")
                    print(f"  Min: {avg.get('min', 0):.2f}%")
                    print(f"  Max: {avg.get('max', 0):.2f}%")
                    print(f"  Data points: {cpu_hist.get('data_points', 0)}")
                    print(f"  Sample interval: {cpu_hist.get('sample_interval_minutes', 5)} minutes")
                
                # Show sample of hourly data
                hourly_data = cpu_hist.get('hourly_data', [])
                if hourly_data:
                    print(f"\n  Sample of collected data (first 5 entries):")
                    for i, entry in enumerate(hourly_data[:5]):
                        timestamp = entry.get('timestamp', 'N/A')
                        cpu = entry.get('cpu_percent', 'N/A')
                        mem = entry.get('memory_available_mb', 'N/A')
                        disk = entry.get('disk_percent', 'N/A')
                        print(f"    {timestamp}: CPU={cpu}% Memory={mem}MB Disk={disk}%")
                    
                    if len(hourly_data) > 5:
                        print(f"    ... and {len(hourly_data) - 5} more data points")
                    print(f"\n  Full data available in JSON output")
            else:
                error_msg = cpu_hist.get('error', 'Not available')
                note_msg = cpu_hist.get('note', '')
                print(f"\nPrevious Day CPU Utilization: {error_msg}")
                if note_msg:
                    print(f"  Note: {note_msg}")
        
        # Services
        if 'services' in self.inventory and isinstance(self.inventory['services'], list):
            running_services = [s for s in self.inventory['services'] if s.get('state') == 'running']
            print(f"\nRunning Services: {len(running_services)}")
        
        # Service dependencies
        if 'service_dependencies' in self.inventory and isinstance(self.inventory['service_dependencies'], dict):
            services_with_deps = [(name, len(info.get('dependencies', []))) 
                                 for name, info in self.inventory['service_dependencies'].items()
                                 if info.get('status') == 'running']
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
        
        # Port dependencies
        if 'port_dependencies' in self.inventory:
            port_deps = self.inventory['port_dependencies']
            listening = port_deps.get('listening_services', {})
            print(f"\nPort-Level Dependencies:")
            print(f"  - Listening ports: {len(listening)}")
            
            if listening:
                print("\n  Key Listening Services:")
                for port in sorted([int(p) for p in listening.keys()])[:10]:
                    port = str(port)
                    info = listening[port]
                    clients = len(info.get('clients', []))
                    processes = info.get('processes', [])
                    process_names = ', '.join([p.get('name', 'unknown') for p in processes[:2]])
                    print(f"    - Port {port}: {process_names} ({info.get('well_known_service')}) - {clients} clients")
        
        # Docker
        if 'docker' in self.inventory and self.inventory['docker'].get('installed'):
            docker = self.inventory['docker']
            print(f"\nDocker:")
            print(f"  - Containers: {len(docker.get('containers', []))}") 
            print(f"  - Images: {len(docker.get('images', []))}")
            print(f"  - Networks: {len(docker.get('networks', []))}")
        
        # Firewall
        if 'firewall' in self.inventory:
            firewall = self.inventory['firewall']
            if firewall.get('profiles'):
                print(f"\nFirewall Profiles:")
                for profile, info in firewall['profiles'].items():
                    print(f"  - {profile.capitalize()}: {info.get('state', 'unknown')}")
        
        print("\n" + "="*80)


def main():
    import argparse
    
    # If running without console and no arguments provided, set default behavior
    if NO_CONSOLE and len(sys.argv) == 1:
        # Default output location when running silently
        default_output = os.path.join(
            os.path.expanduser('~'), 
            'AppData', 
            'Local', 
            'WindowsServerInventory',
            f'inventory_{datetime.datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        )
        sys.argv.extend(['-o', default_output])
    
    parser = argparse.ArgumentParser(
        description='Enhanced Windows Server Inventory Discovery Tool with Performance Monitor',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This tool collects comprehensive Windows server information including:
  - Service dependencies
  - Application-to-application communication mapping
  - Port-level dependencies and service-to-port relationships
  - Network connections and firewall rules
  - Docker container information (if installed)
  - Previous day CPU utilization (requires Performance Monitor configuration)
  - Automatic Performance Monitor configuration via CLI
  - Automatic scheduling via Windows Task Scheduler
  
Example usage:
  python script.py -o inventory.json
  python script.py --summary
  python script.py -o inventory.json -s
  python script.py --install-task
  python script.py --uninstall-task
  python script.py --task-status
  python script.py --enable-perfmon
  python script.py --perfmon-status
  python script.py --disable-perfmon
  
Note: Run as Administrator for complete information and task scheduling
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
        '--install-task',
        action='store_true',
        help='Install scheduled task to run inventory every 6 hours'
    )
    parser.add_argument(
        '--uninstall-task',
        action='store_true',
        help='Uninstall the scheduled task'
    )
    parser.add_argument(
        '--task-status',
        action='store_true',
        help='Check if scheduled task exists and show its details'
    )
    parser.add_argument(
        '--enable-perfmon',
        action='store_true',
        help='Enable Performance Monitor data collection for CPU utilization'
    )
    parser.add_argument(
        '--perfmon-status',
        action='store_true',
        help='Check Performance Monitor data collector status'
    )
    parser.add_argument(
        '--disable-perfmon',
        action='store_true',
        help='Disable and remove Performance Monitor data collector'
    )
    
    args = parser.parse_args()
    
    # ========================================================================
    # AUTOMATIC SCHEDULER SETUP - RUNS ON EVERY EXECUTION
    # ========================================================================
    print("\n" + "="*80)
    print("AUTOMATIC SCHEDULER CHECK")
    print("="*80)
    auto_setup_scheduled_task()
    print("="*80 + "\n")
    sys.stdout.flush()
    
    inventory = WindowsServerInventory()
    
    # Handle Performance Monitor commands
    if args.enable_perfmon:
        print("\n" + "="*80)
        print("ENABLING PERFORMANCE MONITOR DATA COLLECTION")
        print("="*80 + "\n")
        
        if not inventory.is_admin():
            print("ERROR: Administrator privileges required to configure Performance Monitor")
            print("Please run this script as Administrator")
            return 1
        
        result = inventory.setup_performance_monitor()
        
        if result['success']:
            print("\n" + "="*80)
            print("SUCCESS!")
            print("="*80)
            print(result['message'])
            print("\nCommands executed:")
            for cmd in result['commands_executed']:
                print(f"  {cmd}")
        else:
            print("\n" + "="*80)
            print("FAILED")
            print("="*80)
            print(result['message'])
        
        return 0 if result['success'] else 1
    
    if args.perfmon_status:
        print("\n" + "="*80)
        print("PERFORMANCE MONITOR STATUS")
        print("="*80 + "\n")
        
        status = inventory.check_performance_monitor_status()
        
        if status['exists']:
            state = 'Running' if status['running'] else 'Stopped'
            print(f"✓ Data collector 'SystemInventoryCPU' exists")
            print(f"  Status: {state}")
            
            if status['output']:
                print("\nDetails:")
                print("-" * 40)
                print(status['output'])
            
            if not status['running']:
                print("\nTo start the collector, run:")
                print("  python script.py --enable-perfmon")
        else:
            print(f"✗ Data collector 'SystemInventoryCPU' not configured")
            print("\nTo configure Performance Monitor data collection, run:")
            print("  python script.py --enable-perfmon")
        
        return 0
    
    if args.disable_perfmon:
        print("\n" + "="*80)
        print("DISABLING PERFORMANCE MONITOR")
        print("="*80 + "\n")
        
        if not inventory.is_admin():
            print("ERROR: Administrator privileges required")
            print("Please run this script as Administrator")
            return 1
        
        result = inventory.delete_performance_monitor()
        
        if result['success']:
            print(f"✓ {result['message']}")
        else:
            print(f"✗ {result['message']}")
        
        return 0 if result['success'] else 1
    
    # Handle task management commands
    if args.install_task:
        print("\n" + "="*80)
        print("INSTALLING SCHEDULED TASK")
        print("="*80 + "\n")
        
        if not ctypes.windll.shell32.IsUserAnAdmin():
            print("ERROR: Administrator privileges required to install scheduled task")
            print("Please run this script as Administrator")
            return 1
        
        if task_exists():
            print(f"Task '{TASK_NAME}' already exists. It will be replaced.")
        
        if create_task():
            print("\nScheduled task installed successfully!")
            print(f"The inventory will be collected automatically every {INTERVAL_HOURS} hours.")
            
            # Show task details
            task_info = get_task_info()
            if task_info:
                print("\nTask Details:")
                print("-" * 40)
                for line in task_info.split('\n'):
                    if line.strip() and any(key in line for key in ['Task Name', 'Status', 'Next Run Time', 'Last Run Time', 'Task To Run']):
                        print(line)
        return 0
    
    if args.uninstall_task:
        print("\n" + "="*80)
        print("UNINSTALLING SCHEDULED TASK")
        print("="*80 + "\n")
        
        if not ctypes.windll.shell32.IsUserAnAdmin():
            print("ERROR: Administrator privileges required to uninstall scheduled task")
            print("Please run this script as Administrator")
            return 1
        
        if not task_exists():
            print(f"Task '{TASK_NAME}' does not exist.")
            return 0
        
        if delete_task():
            print("\nScheduled task uninstalled successfully!")
        return 0
    
    if args.task_status:
        print("\n" + "="*80)
        print("SCHEDULED TASK STATUS")
        print("="*80 + "\n")
        
        if task_exists():
            print(f"✓ Task '{TASK_NAME}' is installed")
            print(f"  Runs every {INTERVAL_HOURS} hours")
            
            task_info = get_task_info()
            if task_info:
                print("\nTask Details:")
                print("-" * 40)
                for line in task_info.split('\n'):
                    if line.strip() and any(key in line for key in ['Status', 'Next Run Time', 'Last Run Time', 'Last Result', 'Task To Run']):
                        print(line)
        else:
            print(f"✗ Task '{TASK_NAME}' is NOT installed")
            print(f"\nTo install the task, run:")
            print(f"  python {get_exe_path()} --install-task")
        
        print("\n" + "="*80)
        return 0
    
    # Regular inventory collection
    if not inventory.is_admin():
        print("\n" + "="*80)
        print("WARNING: Not running as administrator")
        print("="*80)
        print("Some information may be incomplete.")
        print("Right-click on Command Prompt or PowerShell and select 'Run as administrator'")
        print("="*80 + "\n")
    
    inventory.collect_all()
    
    if args.summary:
        inventory.print_summary()
    elif args.output:
        inventory.save_to_file(args.output)
        if args.summary:
            inventory.print_summary()
    else:
        inventory.save_to_file("inventory.json")
    
    return 0


if __name__ == '__main__':
    main()
