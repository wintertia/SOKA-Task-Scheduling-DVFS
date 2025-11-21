from __future__ import annotations

from collections import namedtuple
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

VM = namedtuple("VM", ["name", "ip", "cpu_cores", "ram_gb"])
Task = namedtuple("Task", ["id", "name", "index", "cpu_load"])


# (threshold_utilization, frequency_ratio)
DVFS_LEVELS: Tuple[Tuple[float, float], ...] = (
	(0.30, 0.50),
	(0.60, 0.70),
	(0.85, 0.85),
	(1.00, 1.00),
)

_MIN_FREQ_RATIO = 1e-3


class DvfsController:
	"""Controller sederhana untuk memilih rasio frekuensi DVFS."""

	def __init__(self, levels: Iterable[Tuple[float, float]] | None = None):
		self.levels = tuple(levels) if levels is not None else DVFS_LEVELS

	def pick_level(self, utilization: float) -> float:
		"""Mengembalikan rasio frekuensi berdasarkan utilisasi host (0..1)."""

		utilization = max(0.0, utilization)
		for threshold, ratio in self.levels:
			if utilization <= threshold:
				return ratio
		return self.levels[-1][1]


@dataclass
class VmState:
	"""Menjaga state utilisasi & waktu prediksi untuk tiap VM."""

	vm: VM
	capacity_reference: float
	total_requested_load: float = 0.0
	projected_time: float = 0.0

	def predict_utilization(self, incoming_load: float) -> float:
		return (self.total_requested_load + incoming_load) / max(self.capacity_reference, 1.0)

	def predict_finish_time(self, incoming_load: float, freq_ratio: float) -> float:
		cpu_capacity = max(self.vm.cpu_cores * max(freq_ratio, _MIN_FREQ_RATIO), _MIN_FREQ_RATIO)
		scaled_time = incoming_load / cpu_capacity
		return self.projected_time + scaled_time

	def apply_assignment(self, task: Task, freq_ratio: float):
		cpu_capacity = max(self.vm.cpu_cores * max(freq_ratio, _MIN_FREQ_RATIO), _MIN_FREQ_RATIO)
		scaled_time = task.cpu_load / cpu_capacity
		self.total_requested_load += task.cpu_load
		self.projected_time += scaled_time


def schedule_with_dvfs(tasks: List[Task], vms: List[VM], controller: DvfsController | None = None) -> Dict[int, str]:
	"""Menjadwalkan tugas ke VM menggunakan heuristik DVFS."""

	if not tasks:
		return {}
	if not vms:
		raise ValueError("Daftar VM kosong: tidak dapat melakukan penjadwalan.")

	controller = controller or DvfsController()
	max_task_load = max(task.cpu_load for task in tasks)
	print(f"Menjalankan DVFS Scheduler untuk {len(tasks)} tugas di {len(vms)} VM...")

	vm_states: Dict[str, VmState] = {
		vm.name: VmState(vm=vm, capacity_reference=max(vm.cpu_cores * max_task_load, 1.0))
		for vm in vms
	}

	assignment: Dict[int, str] = {}

	# Urutkan dari beban terbesar agar keputusan DVFS lebih stabil.
	for task in sorted(tasks, key=lambda t: t.cpu_load, reverse=True):
		best_choice = None

		for vm_state in vm_states.values():
			utilization = vm_state.predict_utilization(task.cpu_load)
			freq_ratio = controller.pick_level(utilization)
			finish_time = vm_state.predict_finish_time(task.cpu_load, freq_ratio)

			if best_choice is None or finish_time < best_choice[2]:
				best_choice = (vm_state, freq_ratio, finish_time)

		if best_choice is None:
			raise RuntimeError("Gagal menentukan VM terbaik untuk tugas")

		selected_vm, freq_ratio, _ = best_choice
		selected_vm.apply_assignment(task, freq_ratio)
		assignment[task.id] = selected_vm.vm.name

	predicted_makespan = max(state.projected_time for state in vm_states.values())
	print(f"Perkiraan makespan DVFS: {predicted_makespan:.4f} satuan waktu")

	return assignment


__all__ = ["schedule_with_dvfs", "DVFS_LEVELS", "DvfsController", "VM", "Task"]
