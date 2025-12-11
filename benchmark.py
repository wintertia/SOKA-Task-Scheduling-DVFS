import asyncio
import csv
import time
import httpx
import pandas as pd
import sys
import os
from typing import Dict, List, Any

import scheduler

# --- KONFIGURASI ---
ITERATION_COUNT = 10
OUTPUT_FILE = 'benchmark.csv'
DATASETS = [
    'dataset.txt',
    'dataset_random.txt',
    'dataset_low_high.txt',
    'dataset_random_stratified.txt'
]

METRIC_FIELDS = [
    "makespan",
    "throughput",
    "total_cpu_time",
    "total_wait_time",
    "imbalance_degree",
    "resource_utilization",
]

# --- FUNGSI HELPER ---

def compute_metrics_single_run(results_list: List[dict], vms: List[scheduler.VM], total_schedule_time: float) -> Dict[str, float]:
    """Menghitung metrik untuk satu kali jalan."""
    if not results_list:
        return {}

    df = pd.DataFrame(results_list)
    
    # Filter tugas yang gagal
    success_df = df[df.get("exec_time", -1) > 0].copy()
    if success_df.empty:
        return {}

    num_tasks = len(success_df)
    total_cpu_time = float(success_df["exec_time"].sum())
    total_wait_time = float(success_df["wait_time"].sum())

    makespan = float(total_schedule_time)
    throughput = float(num_tasks / makespan) if makespan > 0 else 0.0

    # Imbalance
    vm_exec_times = success_df.groupby("vm_assigned")["exec_time"].sum()
    max_load = float(vm_exec_times.max())
    min_load = float(vm_exec_times.min())
    avg_load = float(vm_exec_times.mean())
    imbalance_degree = (max_load - min_load) / avg_load if avg_load > 0 else 0.0

    # Resource Util
    total_cores = sum(vm.cpu_cores for vm in vms)
    total_available_cpu_time = makespan * total_cores
    resource_utilization = total_cpu_time / total_available_cpu_time if total_available_cpu_time > 0 else 0.0

    return {
        "makespan": makespan,
        "throughput": throughput,
        "total_cpu_time": total_cpu_time,
        "total_wait_time": total_wait_time,
        "imbalance_degree": imbalance_degree,
        "resource_utilization": resource_utilization,
    }

async def run_single_iteration(scheduler_fn, tasks: List[scheduler.Task], vms: List[scheduler.VM]) -> Dict[str, float]:
    """Menjalankan satu iterasi penuh (Scheduling + Eksekusi HTTP)."""
    
    # 1. Scheduling
    try:
        assignments = scheduler_fn(tasks, vms)
    except Exception as e:
        print(f"    [Error Scheduler]: {e}")
        return {}

    # 2. Persiapan Eksekusi
    task_lookup = {task.id: task for task in tasks}
    vm_lookup = {vm.name: vm for vm in vms}
    vm_semaphores = {vm.name: asyncio.Semaphore(vm.cpu_cores) for vm in vms}
    results_list: List[dict] = []

    # 3. Eksekusi HTTP Async
    async with httpx.AsyncClient() as client:
        coroutines = [
            scheduler.execute_task_on_vm(
                task_lookup[task_id],
                vm_lookup[vm_name],
                client,
                vm_semaphores[vm_name],
                results_list,
            )
            for task_id, vm_name in assignments.items()
        ]
        
        start_time = time.monotonic()
        await asyncio.gather(*coroutines)
        total_time = time.monotonic() - start_time

    # 4. Hitung Metrik
    return compute_metrics_single_run(results_list, vms, total_time)

# --- FUNGSI UTAMA ---

async def main():
    print(f"=== BENCHMARK ===")
    
    # Setup VM Specs (statis)
    base_vms = [scheduler.VM(name, spec['ip'], spec['cpu'], spec['ram_gb']) 
                for name, spec in scheduler.VM_SPECS.items()]
    
    final_summary_data = []

    # 1. Loop Dataset
    for dataset_file in DATASETS:
        if not os.path.exists(dataset_file):
            print(f"\n[!] File '{dataset_file}' tidak ditemukan. Skip.")
            continue
            
        print(f"\n>> DATASET: {dataset_file}")
        tasks = scheduler.load_tasks(dataset_file)
        if not tasks: 
            continue

        # 2. Loop Algoritma
        for algo_key in scheduler.ALGORITHM_ORDER:
            config = scheduler.ALGORITHMS[algo_key]
            algo_label = config['label']
            runner = config['runner']
            
            print(f"   > Algoritma: {algo_label}")
            print(f"     Running {ITERATION_COUNT} iterasi...", end=" ", flush=True)

            metrics_history = []

            # 3. Loop Iterasi (10x)
            for i in range(ITERATION_COUNT):
                metrics = await run_single_iteration(runner, tasks, base_vms)
                if metrics:
                    metrics_history.append(metrics)
                    print(".", end="", flush=True)
                else:
                    print("x", end="", flush=True) # x menandakan error

            print(" Selesai.")

            # 4. Hitung Rata-Rata (Averaging)
            if metrics_history:
                df_hist = pd.DataFrame(metrics_history)
                avg_metrics = {}
                
                # Hitung mean untuk kolom angka
                for field in METRIC_FIELDS:
                    if field in df_hist.columns:
                        avg_metrics[f"avg_{field}"] = df_hist[field].mean()
                
                # Tambahkan info identitas
                avg_metrics['dataset'] = dataset_file
                avg_metrics['algorithm'] = algo_key
                avg_metrics['num_tasks'] = len(tasks)
                avg_metrics['iterations_count'] = len(metrics_history)
                
                final_summary_data.append(avg_metrics)

    # 5. Tampilkan & Simpan Hasil Akhir
    if not final_summary_data:
        print("\nTidak ada data yang berhasil dikumpulkan.")
        return

    df_final = pd.DataFrame(final_summary_data)
    
    # Susun ulang kolom agar enak dibaca
    cols_order = ['dataset', 'algorithm', 'num_tasks'] + [c for c in df_final.columns if c.startswith('avg_')]
    df_final = df_final[cols_order]

    print("\n\n=== HASIL RATA-RATA BENCHMARK ===")
    # Format tampilan float
    pd.options.display.float_format = '{:,.4f}'.format
    print(df_final.to_string(index=False))

    try:
        df_final.to_csv(OUTPUT_FILE, index=False)
        print(f"\nLaporan lengkap disimpan ke: {OUTPUT_FILE}")
    except Exception as e:
        print(f"Gagal menyimpan CSV: {e}")

if __name__ == "__main__":
    asyncio.run(main())
