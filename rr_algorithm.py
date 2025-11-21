from __future__ import annotations

from collections import namedtuple
from typing import Dict, List, Sequence

VM = namedtuple("VM", ["name", "ip", "cpu_cores", "ram_gb"])
Task = namedtuple("Task", ["id", "name", "index", "cpu_load"])


def _build_rr_sequence(vms: Sequence[VM], weight_by_cores: bool) -> List[VM]:
    sequence: List[VM] = []
    for vm in vms:
        weight = vm.cpu_cores if weight_by_cores else 1
        sequence.extend([vm] * max(1, int(weight)))
    return sequence


def schedule_with_round_robin(
    tasks: List[Task],
    vms: List[VM],
    *,
    weight_by_cores: bool = True,
    start_index: int = 0,
) -> Dict[int, str]:
    """Menjadwalkan tugas ke VM menggunakan Round Robin (opsional berbobot)."""

    if not tasks:
        return {}
    if not vms:
        raise ValueError("Daftar VM kosong: tidak dapat melakukan penjadwalan.")

    rr_sequence = _build_rr_sequence(vms, weight_by_cores)
    if not rr_sequence:
        raise RuntimeError("Tidak ada VM valid untuk Round Robin")

    start_index = start_index % len(rr_sequence)

    assignment: Dict[int, str] = {}
    idx = start_index
    for task in tasks:
        vm = rr_sequence[idx]
        assignment[task.id] = vm.name
        idx = (idx + 1) % len(rr_sequence)

    return assignment


__all__ = ["schedule_with_round_robin", "VM", "Task"]
