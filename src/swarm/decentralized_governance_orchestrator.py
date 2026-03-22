import asyncio
from dataclasses import dataclass
from typing import Dict, List, Optional
import logging
from datetime import datetime

@dataclass
class NodeHealth:
    cpu_usage: float
    memory_usage: float
    last_heartbeat: datetime
    active_tasks: int

@dataclass
class Task:
    id: str
    priority: int
    resource_requirements: Dict[str, float]
    assigned_node: Optional[str] = None

class DecentralizedGovernanceOrchestrator:
    def __init__(self):
        self.nodes: Dict[str, NodeHealth] = {}
        self.tasks: List[Task] = []
        self.health_check_interval = 30  # seconds
        self.logger = logging.getLogger(__name__)

    async def register_node(self, node_id: str, initial_health: NodeHealth):
        """Register a new node in the swarm"""
        self.nodes[node_id] = initial_health
        self.logger.info(f"Node {node_id} registered with initial health: {initial_health}")

    async def update_node_health(self, node_id: str, health: NodeHealth):
        """Update health metrics for a node"""
        if node_id in self.nodes:
            self.nodes[node_id] = health
            await self._check_rebalancing_needed(node_id)

    async def _check_rebalancing_needed(self, node_id: str):
        """Check if workload rebalancing is needed based on node health"""
        node = self.nodes[node_id]
        
        # Define health thresholds
        CPU_THRESHOLD = 80.0
        MEMORY_THRESHOLD = 85.0

        if node.cpu_usage > CPU_THRESHOLD or node.memory_usage > MEMORY_THRESHOLD:
            await self._rebalance_workload(node_id)

    async def _rebalance_workload(self, overloaded_node_id: str):
        """Redistribute tasks from overloaded node to healthier nodes"""
        tasks_to_move = [t for t in self.tasks if t.assigned_node == overloaded_node_id]
        healthy_nodes = [
            (node_id, health) 
            for node_id, health in self.nodes.items()
            if health.cpu_usage < 70 and health.memory_usage < 75
        ]

        if not healthy_nodes:
            self.logger.warning("No healthy nodes available for rebalancing")
            return

        for task in tasks_to_move:
            # Find best node for task based on current load
            best_node = min(healthy_nodes, key=lambda x: x[1].cpu_usage)
            task.assigned_node = best_node[0]
            self.logger.info(
                f"Rebalancing: Moving task {task.id} from {overloaded_node_id} "
                f"to {best_node[0]}"
            )

    async def health_monitor_loop(self):
        """Continuous health monitoring loop"""
        while True:
            try:
                # Check for inactive nodes
                current_time = datetime.now()
                inactive_nodes = [
                    node_id for node_id, health in self.nodes.items()
                    if (current_time - health.last_heartbeat).seconds > 60
                ]

                # Remove inactive nodes and reassign their tasks
                for node_id in inactive_nodes:
                    self.logger.warning(f"Node {node_id} appears to be inactive, removing")
                    del self.nodes[node_id]
                    await self._reassign_tasks_from_node(node_id)

                await asyncio.sleep(self.health_check_interval)

            except Exception as e:
                self.logger.error(f"Error in health monitor loop: {e}")
                await asyncio.sleep(5)  # Brief pause before retry

    async def _reassign_tasks_from_node(self, failed_node_id: str):
        """Reassign tasks from a failed node to other healthy nodes"""
        orphaned_tasks = [t for t in self.tasks if t.assigned_node == failed_node_id]
        
        if not orphaned_tasks:
            return

        available_nodes = list(self.nodes.items())
        if not available_nodes:
            self.logger.error("No available nodes to reassign tasks")
            return

        # Sort tasks by priority
        orphaned_tasks.sort(key=lambda x: x.priority, reverse=True)

        for task in orphaned_tasks:
            # Find least loaded node
            best_node = min(available_nodes, key=lambda x: x[1].active_tasks)
            task.assigned_node = best_node[0]
            self.logger.info(
                f"Reassigned task {task.id} from failed node {failed_node_id} "
                f"to {best_node[0]}"
            )

    async def start(self):
        """Start the orchestrator"""
        self.logger.info("Starting Decentralized Governance Orchestrator")
        await asyncio.gather(
            self.health_monitor_loop()
        )
