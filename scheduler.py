import asyncio
import httpx
import time
from datetime import datetime
import csv
import pandas as pd
import sys
import os
from typing import Callable
from dotenv import load_dotenv
from collections import namedtuple
from dvfs_algorithm import schedule_with_dvfs
from shc_algorithm import stochastic_hill_climb, Task as ShcTask, VM as ShcVM
from rr_algorithm import schedule_with_round_robin

# --- Konfigurasi Lingkungan ---

load_dotenv()

VM_SPECS = {
    'vm1': {'ip': os.getenv("VM1_IP"), 'cpu': 1, 'ram_gb': 1},
    'vm2': {'ip': os.getenv("VM2_IP"), 'cpu': 2, 'ram_gb': 2},
    'vm3': {'ip': os.getenv("VM3_IP"), 'cpu': 4, 'ram_gb': 4},
    'vm4': {'ip': os.getenv("VM4_IP"), 'cpu': 8, 'ram_gb': 4},
}

VM_PORT = 5000
DATASET_FILE = 'dataset.txt'
RESULTS_FILE_TEMPLATE = '{}_results.csv'

VM = namedtuple('VM', ['name', 'ip', 'cpu_cores', 'ram_gb'])
Task = namedtuple('Task', ['id', 'name', 'index', 'cpu_load'])
SchedulerFn = Callable[[list[Task], list[VM]], dict[int, str]]

# --- Definisi Algoritma Penjadwalan ---

DEFAULT_SHC_ITERATIONS = 1000
def schedule_with_shc(tasks: list[Task], vms: list[VM], iterations: int = DEFAULT_SHC_ITERATIONS) -> dict[int, str]:
    """Adaptor untuk menjalankan SHC dengan struktur Task/VM lokal."""

    shc_tasks = [ShcTask(task.id, task.name, task.index, task.cpu_load, 0) for task in tasks]
    shc_vms = [ShcVM(vm.name, vm.ip, vm.cpu_cores, vm.ram_gb) for vm in vms]
    return stochastic_hill_climb(shc_tasks, shc_vms, iterations)

ALGORITHM_ORDER = ['dvfs', 'rr', 'shc']
ALGORITHMS: dict[str, dict[str, object]] = {
    'dvfs': {
        'label': 'DVFS (Dynamic Voltage and Frequency Scaling)',
        'runner': schedule_with_dvfs,
    },
    'rr': {
        'label': 'Round Robin (weighted by CPU cores)',
        'runner': schedule_with_round_robin,
    },
    'shc': {
        'label': f'Stochastic Hill Climbing ({DEFAULT_SHC_ITERATIONS} iterasi)',
        'runner': schedule_with_shc,
    },
}


def prompt_algorithm_choice() -> tuple[str, str, SchedulerFn]:
    print("\nPilih Algoritma Penjadwalan:")
    for idx, key in enumerate(ALGORITHM_ORDER, start=1):
        label = ALGORITHMS[key]['label']
        print(f"  {idx}. {label}")

    selection = input("Masukkan pilihan [1]: ").strip()
    try:
        selected_index = int(selection) - 1 if selection else 0
    except ValueError:
        selected_index = 0

    if selected_index < 0 or selected_index >= len(ALGORITHM_ORDER):
        selected_index = 0

    key = ALGORITHM_ORDER[selected_index]
    config = ALGORITHMS[key]
    return key, config['label'], config['runner']  # type: ignore[return-value]

# --- Fungsi Helper & Definisi Task ---

def get_task_load(index: int):
    cpu_load = (index * index * 10000)
    return cpu_load

def load_tasks(dataset_path: str) -> list[Task]:
    if not os.path.exists(dataset_path):
        print(f"Error: File dataset '{dataset_path}' tidak ditemukan.", file=sys.stderr)
        sys.exit(1)
        
    tasks = []
    with open(dataset_path, 'r') as f:
        for i, line in enumerate(f):
            try:
                index = int(line.strip())
                if not 1 <= index <= 10:
                    print(f"Peringatan: Task index {index} di baris {i+1} di luar rentang (1-10).")
                    continue
                
                cpu_load = get_task_load(index)
                task_name = f"task-{index}-{i}"
                tasks.append(Task(
                    id=i,
                    name=task_name,
                    index=index,
                    cpu_load=cpu_load,
                ))
            except ValueError:
                print(f"Peringatan: Mengabaikan baris {i+1} yang tidak valid: '{line.strip()}'")
    
    print(f"Berhasil memuat {len(tasks)} tugas dari {dataset_path}")
    return tasks

# --- Eksekutor Tugas Asinkron ---

async def execute_task_on_vm(task: Task, vm: VM, client: httpx.AsyncClient, 
                            vm_semaphore: asyncio.Semaphore, results_list: list):
    """
    Mengirim request GET ke VM yang ditugaskan, dibatasi oleh semaphore VM.
    Mencatat hasil dan waktu.
    """
    url = f"http://{vm.ip}:{VM_PORT}/task/{task.index}"
    task_start_time = None
    task_finish_time = None
    task_exec_time = -1.0
    task_wait_time = -1.0
    
    wait_start_mono = time.monotonic()
    
    try:
        async with vm_semaphore:
            # Waktu tunggu selesai, eksekusi dimulai
            task_wait_time = time.monotonic() - wait_start_mono
            
            print(f"Mengeksekusi {task.name} (idx: {task.id}) di {vm.name} (IP: {vm.ip})...")
            
            # Catat waktu mulai
            task_start_mono = time.monotonic()
            task_start_time = datetime.now()
            
            # Kirim request GET
            response = await client.get(url, timeout=300.0) # Timeout 5 menit
            response.raise_for_status()
            
            # Catat waktu selesai
            task_finish_time = datetime.now()
            task_exec_time = time.monotonic() - task_start_mono
            
            print(f"Selesai {task.name} (idx: {task.id}) di {vm.name}. Waktu: {task_exec_time:.4f}s")
            
    except httpx.HTTPStatusError as e:
        print(f"Error HTTP pada {task.name} di {vm.name}: {e}", file=sys.stderr)
    except httpx.RequestError as e:
        print(f"Error Request pada {task.name} di {vm.name}: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Error tidak diketahui pada {task.name} di {vm.name}: {e}", file=sys.stderr)
        
    finally:
        if task_start_time is None:
            task_start_time = datetime.now()
        if task_finish_time is None:
            task_finish_time = datetime.now()
            
        results_list.append({
            "index": task.id,
            "task_name": task.name,
            "vm_assigned": vm.name,
            "start_time": task_start_time,
            "exec_time": task_exec_time,
            "finish_time": task_finish_time,
            "wait_time": task_wait_time
        })

# --- Fungsi Paska-Proses & Metrik ---

def write_results_to_csv(results_list: list, results_file: str):
    """Menyimpan hasil eksekusi ke file CSV."""
    if not results_list:
        print("Tidak ada hasil untuk ditulis ke CSV.", file=sys.stderr)
        return

    # Urutkan berdasarkan 'index' untuk keterbacaan
    results_list.sort(key=lambda x: x['index'])

    headers = ["index", "task_name", "vm_assigned", "start_time", "exec_time", "finish_time", "wait_time"]
    
    # Format datetime agar lebih mudah dibaca di CSV
    formatted_results = []
    min_start = min(item['start_time'] for item in results_list)
    for r in results_list:
        new_r = r.copy()
        new_r['start_time'] = (r['start_time'] - min_start).total_seconds()
        new_r['finish_time'] = (r['finish_time'] - min_start).total_seconds()
        formatted_results.append(new_r)

    formatted_results.sort(key=lambda item: item['start_time'])

    try:
        with open(results_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(formatted_results)
        print(f"\nData hasil eksekusi disimpan ke {results_file}")
    except IOError as e:
        print(f"Error menulis ke CSV {results_file}: {e}", file=sys.stderr)

def calculate_and_print_metrics(results_list: list, vms: list[VM], total_schedule_time: float):
    try:
        df = pd.DataFrame(results_list)
    except pd.errors.EmptyDataError:
        print("Error: Hasil kosong, tidak ada metrik untuk dihitung.", file=sys.stderr)
        return

    # Konversi kolom waktu
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['finish_time'] = pd.to_datetime(df['finish_time'])
    
    # Filter 'failed' tasks (exec_time < 0)
    success_df = df[df['exec_time'] > 0].copy()
    
    if success_df.empty:
        print("Tidak ada tugas yang berhasil diselesaikan. Metrik tidak dapat dihitung.")
        return

    num_tasks = len(success_df)
    
    # Hitung metrik
    total_cpu_time = success_df['exec_time'].sum()
    total_wait_time = success_df['wait_time'].sum()
    
    avg_exec_time = success_df['exec_time'].mean()
    avg_wait_time = success_df['wait_time'].mean()
    
    # Waktu mulai & selesai relatif terhadap awal
    min_start = success_df['start_time'].min()
    success_df['rel_start_time'] = (success_df['start_time'] - min_start).dt.total_seconds()
    success_df['rel_finish_time'] = (success_df['finish_time'] - min_start).dt.total_seconds()
    
    avg_start_time = success_df['rel_start_time'].mean()
    avg_finish_time = success_df['rel_finish_time'].mean()
    
    makespan = total_schedule_time # Waktu dari eksekusi pertama hingga terakhir
    throughput = num_tasks / makespan if makespan > 0 else 0
    
    # Imbalance Degree (Degree of Imbalance)
    vm_exec_times = success_df.groupby('vm_assigned')['exec_time'].sum()
    max_load = vm_exec_times.max()
    min_load = vm_exec_times.min()
    avg_load = vm_exec_times.mean()
    imbalance_degree = (max_load - min_load) / avg_load if avg_load > 0 else 0
    
    # Resource Utilization
    total_available_cpu_time = 0
    total_cores = sum(vm.cpu_cores for vm in vms)
    total_available_cpu_time = makespan * total_cores
    resource_utilization = total_cpu_time / total_available_cpu_time if total_available_cpu_time > 0 else 0

    # Tampilkan Metrik
    print("\n--- Hasil ---")
    print(f"Total Tugas Selesai       : {num_tasks}")
    print(f"Makespan (Waktu Total)    : {makespan:.4f} detik")
    print(f"Throughput                : {throughput:.4f} tugas/detik")
    print(f"Total CPU Time            : {total_cpu_time:.4f} detik")
    print(f"Total Wait Time           : {total_wait_time:.4f} detik")
    print(f"Average Start Time (rel)  : {avg_start_time:.4f} detik")
    print(f"Average Execution Time    : {avg_exec_time:.4f} detik")
    print(f"Average Finish Time (rel) : {avg_finish_time:.4f} detik")
    print(f"Imbalance Degree          : {imbalance_degree:.4f}")
    print(f"Resource Utilization (CPU): {resource_utilization:.4%}")

# --- 6. Fungsi Main ---

async def main():
    # 1. Inisialisasi
    vms = [VM(name, spec['ip'], spec['cpu'], spec['ram_gb']) 
            for name, spec in VM_SPECS.items()]
    
    tasks = load_tasks(DATASET_FILE)
    if not tasks:
        print("Tidak ada tugas untuk dijadwalkan. Keluar.", file=sys.stderr)
        return
        
    tasks_dict = {task.id: task for task in tasks}
    vms_dict = {vm.name: vm for vm in vms}

    algorithm_key, algorithm_label, scheduler_fn = prompt_algorithm_choice()
    print(f"\nMenggunakan algoritma: {algorithm_label}")

    # 2. Jalankan Algoritma Penjadwalan
    best_assignment = scheduler_fn(tasks, vms)
    
    print("\nPenugasan Tugas Terbaik Ditemukan:")
    for i in range(min(10, len(best_assignment))): # Tampilkan 10 pertama
        print(f"  - Tugas {i} -> {best_assignment[i]}")
    if len(best_assignment) > 10:
        print("  - ... etc.")

    # 3. Siapkan Eksekusi
    results_list = []
    
    # Buat semaphore untuk setiap VM berdasarkan core CPU
    vm_semaphores = {vm.name: asyncio.Semaphore(vm.cpu_cores) for vm in vms}
    
    # Buat satu HTTP client untuk semua request
    async with httpx.AsyncClient() as client:
        
        # Siapkan semua coroutine tugas
        all_task_coroutines = []
        for task_id, vm_name in best_assignment.items():
            task = tasks_dict[task_id]
            vm = vms_dict[vm_name]
            sem = vm_semaphores[vm_name]
            
            all_task_coroutines.append(
                execute_task_on_vm(task, vm, client, sem, results_list)
            )
            
        print(f"\nMemulai eksekusi {len(all_task_coroutines)} tugas secara paralel...")
        
        # 4. Jalankan Semua Tugas dan Ukur Waktu Total
        schedule_start_time = time.monotonic()
        
        await asyncio.gather(*all_task_coroutines)
        
        schedule_end_time = time.monotonic()
        total_schedule_time = schedule_end_time - schedule_start_time
        
        print(f"\nSemua eksekusi tugas selesai dalam {total_schedule_time:.4f} detik.")
    
    # 5. Simpan Hasil dan Hitung Metrik
    results_file = RESULTS_FILE_TEMPLATE.format(algorithm_key)
    write_results_to_csv(results_list, results_file)
    calculate_and_print_metrics(results_list, vms, total_schedule_time)

if __name__ == "__main__":
    asyncio.run(main())