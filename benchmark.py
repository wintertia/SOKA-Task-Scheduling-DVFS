import asyncio
import csv
import time
import httpx
import pandas as pd
import sys
from typing import Dict, List, Any

import scheduler

# --- Konfigurasi Benchmark ---

BENCHMARK_RESULTS_FILE = 'benchmark_comparison.csv'

# --- Fungsi Helper Metrik (Diadaptasi dari scheduler_10x_runner) ---

def compute_metrics_for_benchmark(results_list: List[dict], vms: List[scheduler.VM], total_schedule_time: float) -> Dict[str, float]:
    """Menghitung metrik kinerja dari hasil eksekusi mentah."""
    if not results_list:
        return {}

    df = pd.DataFrame(results_list)
    df["start_time"] = pd.to_datetime(df["start_time"])
    df["finish_time"] = pd.to_datetime(df["finish_time"])

    success_df = df[df["exec_time"] > 0].copy()
    if success_df.empty:
        return {}

    num_tasks = len(success_df)
    total_cpu_time = float(success_df["exec_time"].sum())
    total_wait_time = float(success_df["wait_time"].sum())

    makespan = float(total_schedule_time)
    throughput = float(num_tasks / makespan) if makespan > 0 else 0.0

    # Imbalance Degree
    vm_exec_times = success_df.groupby("vm_assigned")["exec_time"].sum()
    max_load = float(vm_exec_times.max())
    min_load = float(vm_exec_times.min())
    avg_load = float(vm_exec_times.mean())
    imbalance_degree = (max_load - min_load) / avg_load if avg_load > 0 else 0.0

    # Resource Utilization
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

# --- Eksekusi Benchmark ---

async def run_algorithm_benchmark(algo_key: str, algo_label: str, scheduler_fn: scheduler.SchedulerFn, tasks: List[scheduler.Task], vms: List[scheduler.VM]) -> Dict[str, Any]:
    """Menjalankan satu algoritma dan mengembalikan metriknya."""
    
    print(f"\n--- Menjalankan: {algo_label} ---")
    
    # 1. Jalankan Logika Penjadwalan
    start_sched = time.monotonic()
    try:
        assignments = scheduler_fn(tasks, vms)
    except Exception as e:
        print(f"Gagal menjadwalkan dengan {algo_key}: {e}", file=sys.stderr)
        return {"algorithm": algo_key, "error": str(e)}
    
    sched_duration = time.monotonic() - start_sched
    print(f"  Penjadwalan selesai dalam {sched_duration:.4f}s")

    # 2. Persiapan Eksekusi HTTP
    task_lookup = {task.id: task for task in tasks}
    vm_lookup = {vm.name: vm for vm in vms}
    vm_semaphores = {vm.name: asyncio.Semaphore(vm.cpu_cores) for vm in vms}
    results_list: List[dict] = []

    # 3. Eksekusi Tugas
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
        
        print(f"  Mengeksekusi {len(coroutines)} tugas...")
        schedule_start = time.monotonic()
        await asyncio.gather(*coroutines)
        total_schedule_time = time.monotonic() - schedule_start

    # 4. Hitung Metrik
    metrics = compute_metrics_for_benchmark(results_list, vms, total_schedule_time)
    
    # Tambahkan identitas algoritma
    metrics["algorithm"] = algo_key
    metrics["algorithm_label"] = algo_label
    metrics["scheduling_overhead"] = sched_duration
    
    return metrics

async def main():
    print("=== Memulai Benchmark Perbandingan Semua Algoritma ===")
    
    # 1. Load Data
    vms = [scheduler.VM(name, spec['ip'], spec['cpu'], spec['ram_gb']) 
           for name, spec in scheduler.VM_SPECS.items()]
    tasks = scheduler.load_tasks(scheduler.DATASET_FILE)
    
    if not tasks:
        print("Tidak ada tugas. Keluar.")
        return

    benchmark_data = []

    # 2. Loop Semua Algoritma yang Terdaftar di scheduler.py
    # Urutan: FCFS -> DVFS -> RR -> SHC
    for key in scheduler.ALGORITHM_ORDER:
        config = scheduler.ALGORITHMS[key]
        label = config['label']
        runner = config['runner']
        
        # Jalankan
        result = await run_algorithm_benchmark(key, label, runner, tasks, vms)
        benchmark_data.append(result)

    # 3. Tampilkan Hasil Perbandingan
    print("\n\n=== Hasil Komparasi Benchmark ===")
    
    df = pd.DataFrame(benchmark_data)
    
    # Atur urutan kolom agar mudah dibaca
    cols = ["algorithm", "makespan", "throughput", "total_wait_time", "imbalance_degree", "resource_utilization", "scheduling_overhead"]
    # Filter kolom yang ada saja (jika ada error pada salah satu algo)
    cols = [c for c in cols if c in df.columns]
    
    df_display = df[cols].copy()
    
    # Format tampilan angka
    pd.options.display.float_format = '{:,.4f}'.format
    print(df_display.to_string(index=False))

    # 4. Simpan ke CSV
    try:
        df_display.to_csv(BENCHMARK_RESULTS_FILE, index=False)
        print(f"\nHasil lengkap disimpan ke: {BENCHMARK_RESULTS_FILE}")
    except IOError as e:
        print(f"Gagal menyimpan CSV: {e}")

if __name__ == "__main__":
    asyncio.run(main())