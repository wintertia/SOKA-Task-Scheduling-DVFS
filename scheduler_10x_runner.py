import asyncio
import csv
import time
import os
from typing import Dict, List

import httpx
import pandas as pd

import scheduler

ITERATION_COUNT = 10
METRIC_FIELDS = [
    "total_tasks_completed",
    "makespan",
    "throughput",
    "total_cpu_time",
    "total_wait_time",
    "average_start_time",
    "average_execution_time",
    "average_finish_time",
    "imbalance_degree",
    "resource_utilization",
]

# Daftar dataset yang akan diproses
DATASETS = [
    'dataset.txt',
    'dataset_random.txt',
    'dataset_low_high.txt',
    'dataset_random_stratified.txt'
]


def build_vms() -> List[scheduler.VM]:
    """Construct VM objects from the specs defined in scheduler.py."""
    return [
        scheduler.VM(name, spec["ip"], spec["cpu"], spec["ram_gb"])
        for name, spec in scheduler.VM_SPECS.items()
    ]


def compute_metrics(results_list: List[dict], vms: List[scheduler.VM], total_schedule_time: float) -> Dict[str, float]:
    if not results_list:
        raise RuntimeError("Result list kosong, tidak ada data untuk metrik.")

    df = pd.DataFrame(results_list)
    df["start_time"] = pd.to_datetime(df["start_time"])
    df["finish_time"] = pd.to_datetime(df["finish_time"])

    success_df = df[df["exec_time"] > 0].copy()
    if success_df.empty:
        raise RuntimeError("Tidak ada tugas yang berhasil, metrik tidak tersedia.")

    num_tasks = len(success_df)
    total_cpu_time = float(success_df["exec_time"].sum())
    total_wait_time = float(success_df["wait_time"].sum())

    min_start = success_df["start_time"].min()
    success_df["rel_start_time"] = (success_df["start_time"] - min_start).dt.total_seconds()
    success_df["rel_finish_time"] = (success_df["finish_time"] - min_start).dt.total_seconds()

    avg_start_time = float(success_df["rel_start_time"].mean())
    avg_exec_time = float(success_df["exec_time"].mean())
    avg_finish_time = float(success_df["rel_finish_time"].mean())

    makespan = float(total_schedule_time)
    throughput = float(num_tasks / makespan) if makespan > 0 else 0.0

    vm_exec_times = success_df.groupby("vm_assigned")["exec_time"].sum()
    max_load = float(vm_exec_times.max())
    min_load = float(vm_exec_times.min())
    avg_load = float(vm_exec_times.mean())
    imbalance_degree = (max_load - min_load) / avg_load if avg_load > 0 else 0.0

    total_cores = sum(vm.cpu_cores for vm in vms)
    total_available_cpu_time = makespan * total_cores
    resource_utilization = total_cpu_time / total_available_cpu_time if total_available_cpu_time > 0 else 0.0

    return {
        "total_tasks_completed": float(num_tasks),
        "makespan": makespan,
        "throughput": throughput,
        "total_cpu_time": total_cpu_time,
        "total_wait_time": total_wait_time,
        "average_start_time": avg_start_time,
        "average_execution_time": avg_exec_time,
        "average_finish_time": avg_finish_time,
        "imbalance_degree": imbalance_degree,
        "resource_utilization": resource_utilization,
    }


async def run_single_iteration(algorithm_key: str, scheduler_fn, tasks: List[scheduler.Task]) -> Dict[str, float]:
    vms = build_vms()
    task_lookup = {task.id: task for task in tasks}
    vm_lookup = {vm.name: vm for vm in vms}
    assignments = scheduler_fn(tasks, vms)

    vm_semaphores = {vm.name: asyncio.Semaphore(vm.cpu_cores) for vm in vms}
    results_list: List[dict] = []

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
        schedule_start = time.monotonic()
        await asyncio.gather(*coroutines)
        total_schedule_time = time.monotonic() - schedule_start

    return compute_metrics(results_list, vms, total_schedule_time)


def average_metrics(metrics_history: List[Dict[str, float]]) -> Dict[str, float]:
    df = pd.DataFrame(metrics_history)
    averages: Dict[str, float] = {}

    for field in METRIC_FIELDS:
        if field in df.columns:
            averages[field] = float(df[field].mean())

    if "total_tasks_completed" in averages:
        averages["total_tasks_completed"] = float(round(averages["total_tasks_completed"]))

    return averages


def print_iteration_summary(iteration: int, metrics: Dict[str, float]) -> None:
    print(f"\n  Ringkasan Iterasi {iteration}:")
    print(f"    Makespan              : {metrics['makespan']:.4f} s")
    print(f"    Avg Execution Time    : {metrics['average_execution_time']:.4f} s")
    print(f"    Imbalance Degree      : {metrics['imbalance_degree']:.4f}")
    print(f"    Resource Utilization  : {metrics['resource_utilization']:.4%}")


def save_average_metrics_csv(algorithm_key: str, dataset_path: str, averages: Dict[str, float]) -> str:
    # Ambil nama file tanpa ekstensi (misal: 'dataset_random')
    dataset_name = os.path.splitext(os.path.basename(dataset_path))[0]
    
    # Format nama file: {algoritma}_{dataset}_10x_results.csv
    csv_path = f"{algorithm_key}_{dataset_name}_10x_results.csv"
    
    with open(csv_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["metric", "average_value"])
        for field in METRIC_FIELDS:
            if field in averages:
                writer.writerow([field, averages[field]])
    return csv_path


async def process_dataset(dataset_path: str, algorithm_key: str, algorithm_label: str, scheduler_fn):
    print(f"\n{'='*60}")
    print(f"DATASET: {dataset_path}")
    print(f"ALGORITMA: {algorithm_label}")
    print(f"{'='*60}")

    if not os.path.exists(dataset_path):
        print(f"Error: File dataset '{dataset_path}' tidak ditemukan. Melewati...")
        return

    tasks = scheduler.load_tasks(dataset_path)
    if not tasks:
        print("Tidak ada tugas untuk dieksekusi dalam dataset ini.")
        return

    print(f"Memulai pengujian {ITERATION_COUNT} iterasi untuk {dataset_path} ...")
    
    metrics_history: List[Dict[str, float]] = []
    
    for iteration in range(1, ITERATION_COUNT + 1):
        print(f"\n--- {dataset_path} | Iterasi {iteration}/{ITERATION_COUNT} ---")
        try:
            iteration_metrics = await run_single_iteration(algorithm_key, scheduler_fn, tasks)
            metrics_history.append(iteration_metrics)
            print_iteration_summary(iteration, iteration_metrics)
        except Exception as e:
            print(f"Error pada iterasi {iteration}: {e}")

    if metrics_history:
        averages = average_metrics(metrics_history)
        csv_path = save_average_metrics_csv(algorithm_key, dataset_path, averages)

        print(f"\n>>> Rata-rata Akhir untuk {dataset_path}:")
        for field in METRIC_FIELDS:
            if field in averages:
                value = averages[field]
                if field == "resource_utilization":
                    print(f"  {field:<25}: {value:.4%}")
                else:
                    print(f"  {field:<25}: {value:.4f}")
        print(f"\nHasil rata-rata disimpan di: {csv_path}")
    else:
        print(f"Gagal mengumpulkan metrik untuk {dataset_path}.")


async def main() -> None:
    # Meminta input algoritma SATU KALI di awal
    algorithm_key, algorithm_label, scheduler_fn = scheduler.prompt_algorithm_choice()
    
    # Loop melalui setiap dataset yang didefinisikan
    for dataset_path in DATASETS:
        await process_dataset(dataset_path, algorithm_key, algorithm_label, scheduler_fn)
        
    print("\n\n=== Semua eksperimen dataset selesai ===")


if __name__ == "__main__":
    asyncio.run(main())