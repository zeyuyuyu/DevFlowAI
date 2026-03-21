"""
Decentralized Governance Orchestrator for DevFlowAI Swarm
Implements Byzantine-Fault-Tolerant consensus with reputation-based governance.

Architecture:
- AgentRegistry: Identity & reputation management with staking
- ConsensusEngine: PBFT implementation for swarm decisions  
- GovernanceContract: Proposal lifecycle and voting mechanisms
- SwarmOrchestrator: Coordination and conflict resolution
"""

import asyncio
import hashlib
import json
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Dict, List, Optional, Set, Callable, Any, Tuple, AsyncIterator
from collections import defaultdict
import secrets
import hmac

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AgentStatus(Enum):
    ACTIVE = auto()
    SUSPENDED = auto()
    JAILED = auto()
    OFFLINE = auto()
    BYZANTINE_DETECTED = auto()


class ProposalStatus(Enum):
    PENDING = auto()
    VOTING = auto()
    PASSED = auto()
    REJECTED = auto()
    EXECUTED = auto()
    EXPIRED = auto()


class ConsensusPhase(Enum):
    REQUEST = auto()
    PRE_PREPARE = auto()
    PREPARE = auto()
    COMMIT = auto()
    REPLY = auto()


@dataclass(frozen=True)
class AgentIdentity:
    agent_id: str
    public_key: str
    stake_amount: float
    reputation_score: float = 1.0
    joined_at: datetime = field(default_factory=datetime.utcnow)
    
    def verify_signature(self, message: str, signature: str) -> bool:
        """Verify agent's cryptographic signature."""
        expected = hmac.new(
            self.public_key.encode(),
            message.encode(),
            hashlib.sha256
        ).hexdigest()
        return secrets.compare_digest(expected, signature)


@dataclass
class Proposal:
    proposal_id: str
    proposer_id: str
    title: str
    description: str
    action_type: str
    payload: Dict[str, Any]
    created_at: datetime
    voting_end_time: datetime
    required_quorum: float = 0.66
    execution_threshold: float = 0.50
    status: ProposalStatus = ProposalStatus.PENDING
    votes: Dict[str, bool] = field(default_factory=dict)
    vote_weights: Dict[str, float] = field(default_factory=dict)
    
    @property
    def total_voting_power(self) -> float:
        return sum(self.vote_weights.values())
    
    @property
    def approval_power(self) -> float:
        yes_votes = sum(w for agent, w in self.vote_weights.items() if self.votes.get(agent))
        return yes_votes / self.total_voting_power if self.total_voting_power > 0 else 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            'status': self.status.name,
            'created_at': self.created_at.isoformat(),
            'voting_end_time': self.voting_end_time.isoformat()
        }


@dataclass
class SwarmTask:
    task_id: str
    task_type: str
    complexity: float  # 0.0 to 1.0
    priority: int
    payload: Dict[str, Any]
    assigned_agent: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    deadline: Optional[datetime] = None
    status: str = "pending"
    
    def calculate_reward(self, base_rate: float = 1.0) -> float:
        """Calculate task reward based on complexity and priority."""
        return base_rate * (1 + self.complexity) * self.priority


@dataclass
class ConsensusMessage:
    phase: ConsensusPhase
    view_number: int
    sequence_number: int
    digest: str
    agent_id: str
    signature: str
    timestamp: float = field(default_factory=time.time)
    data: Optional[Dict[str, Any]] = None


class ByzantineFault(Exception):
    """Raised when Byzantine behavior is detected."""
    pass


class InsufficientStake(Exception):
    """Raised when agent has insufficient stake for operation."""
    pass


class QuorumNotReached(Exception):
    """Raised when voting quorum is not achieved."""
    pass


class AgentRegistry:
    """Manages agent identities, stakes, and reputation in the swarm."""
    
    def __init__(self, min_stake: float = 100.0, slashing_rate: float = 0.1):
        self.agents: Dict[str, AgentIdentity] = {}
        self.status: Dict[str, AgentStatus] = {}
        self.reputation_history: Dict[str, List[Tuple[datetime, float]]] = defaultdict(list)
        self.stake_locked: Dict[str, float] = defaultdict(float)
        self.min_stake = min_stake
        self.slashing_rate = slashing_rate
        self._lock = asyncio.Lock()
        
    async def register_agent(self, identity: AgentIdentity) -> bool:
        """Register new agent with stake verification."""
        async with self._lock:
            if identity.stake_amount < self.min_stake:
                raise InsufficientStake(f"Minimum stake required: {self.min_stake}")
            
            self.agents[identity.agent_id] = identity
            self.status[identity.agent_id] = AgentStatus.ACTIVE
            self.reputation_history[identity.agent_id].append(
                (datetime.utcnow(), identity.reputation_score)
            )
            logger.info(f"Agent {identity.agent_id} registered with stake {identity.stake_amount}")
            return True
    
    async def slash_agent(self, agent_id: str, reason: str, severity: float = 1.0) -> float:
        """Slash agent stake for malicious behavior."""
        async with self._lock:
            if agent_id not in self.agents:
                return 0.0
            
            agent = self.agents[agent_id]
            slash_amount = agent.stake_amount * self.slashing_rate * severity
            
            # Update agent with reduced stake
            new_stake = max(0, agent.stake_amount - slash_amount)
            self.agents[agent_id] = AgentIdentity(
                agent_id=agent.agent_id,
                public_key=agent.public_key,
                stake_amount=new_stake,
                reputation_score=max(0, agent.reputation_score - (0.1 * severity))
            )
            
            if new_stake < self.min_stake:
                self.status[agent_id] = AgentStatus.JAILED
                logger.warning(f"Agent {agent_id} jailed due to insufficient stake after slashing")
            
            logger.info(f"Agent {agent_id} slashed by {slash_amount} for: {reason}")
            return slash_amount
    
    async def update_reputation(self, agent_id: str, delta: float, task_id: str = ""):
        """Update agent reputation score."""
        async with self._lock:
            if agent_id not in self.agents:
                return
            
            agent = self.agents[agent_id]
            new_score = max(0, min(5.0, agent.reputation_score + delta))
            
            self.agents[agent_id] = AgentIdentity(
                agent_id=agent.agent_id,
                public_key=agent.public_key,
                stake_amount=agent.stake_amount,
                reputation_score=new_score
            )
            
            self.reputation_history[agent_id].append((datetime.utcnow(), new_score))
            logger.debug(f"Agent {agent_id} reputation updated to {new_score} (task: {task_id})")
    
    def get_active_agents(self) -> List[str]:
        """Get list of active agent IDs."""
        return [
            aid for aid, status in self.status.items() 
            if status == AgentStatus.ACTIVE
        ]
    
    def get_voting_power(self, agent_id: str) -> float:
        """Calculate voting power based on stake and reputation."""
        if agent_id not in self.agents:
            return 0.0
        agent = self.agents[agent_id]
        # Voting power = stake * reputation multiplier
        return agent.stake_amount * (0.5 + (agent.reputation_score / 10))


class ConsensusEngine:
    """Implements Practical Byzantine Fault Tolerance (PBFT) for swarm consensus."""
    
    def __init__(self, registry: AgentRegistry, f_tolerance: int = 1):
        self.registry = registry
        self.f = f_tolerance  # Number of Byzantine faults to tolerate
        self.view_number = 0
        self.sequence_number = 0
        self.prepared_certificates: Dict[int, Set[str]] = {}
        self.committed_certificates: Dict[int, Set[str]] = {}
        self.message_log: List[ConsensusMessage] = []
        self._consensus_lock = asyncio.Lock()
        
    def _create_digest(self, data: Dict[str, Any]) -> str:
        """Create SHA-256 digest of data."""
        return hashlib.sha256(
            json.dumps(data, sort_keys=True).encode()
        ).hexdigest()
    
    async def propose_value(self, value: Dict[str, Any], primary_agent: str) -> Optional[str]:
        """Primary agent proposes a value to the swarm."""
        async with self._consensus_lock:
            active_agents = self.registry.get_active_agents()
            n = len(active_agents)
            
            if n < 3 * self.f + 1:
                raise QuorumNotReached(
                    f"Insufficient agents for BFT: {n} < {3*self.f + 1}"
                )
            
            self.sequence_number += 1
            seq = self.sequence_number
            digest = self._create_digest(value)
            
            # Create PRE-PREPARE message
            pre_prepare = ConsensusMessage(
                phase=ConsensusPhase.PRE_PREPARE,
                view_number=self.view_number,
                sequence_number=seq,
                digest=digest,
                agent_id=primary_agent,
                signature="",  # Would be actual signature in production
                data=value
            )
            
            self.message_log.append(pre_prepare)
            
            # Simulate broadcast to all agents
            prepare_tasks = [
                self._handle_prepare(pre_prepare, agent_id)
                for agent_id in active_agents if agent_id != primary_agent
            ]
            
            results = await asyncio.gather(*prepare_tasks, return_exceptions=True)
            valid_prepares = [r for r in results if isinstance(r, ConsensusMessage)]
            
            # Check if 2f prepares received
            if len(valid_prepares) >= 2 * self.f:
                self.prepared_certificates[seq] = {m.agent_id for m in valid_prepares}
                
                # Move to COMMIT phase
                commit_tasks = [
                    self._handle_commit(pre_prepare, agent_id)
                    for agent_id in active_agents
                ]
                commit_results = await asyncio.gather(*commit_tasks, return_exceptions=True)
                valid_commits = [r for r in commit_results if r is True]
                
                if len(valid_commits) >= 2 * self.f + 1:
                    self.committed_certificates[seq] = self.prepared_certificates[seq]
                    logger.info(f"Consensus reached for sequence {seq}")
                    return digest
            
            return None
    
    async def _handle_prepare(self, msg: ConsensusMessage, agent_id: str) -> Optional[ConsensusMessage]:
        """Agent validates and responds to PRE-PREPARE."""
        # Verify agent is legitimate
        if agent_id not in self.registry.agents:
            return None
        
        # Verify digest
        if self._create_digest(msg.data) != msg.digest:
            await self.registry.slash_agent(agent_id, "Invalid digest in prepare phase", 2.0)
            return None
        
        # Send PREPARE message
        prepare_msg = ConsensusMessage(
            phase=ConsensusPhase.PREPARE,
            view_number=msg.view_number,
            sequence_number=msg.sequence_number,
            digest=msg.digest,
            agent_id=agent_id,
            signature=""
        )
        return prepare_msg
    
    async def _handle_commit(self, msg: ConsensusMessage, agent_id: str) -> bool:
        """Agent commits the value."""
        # Check if prepared certificate exists
        if msg.sequence_number not in self.prepared_certificates:
            return False
        
        if agent_id in self.prepared_certificates[msg.sequence_number]:
            return True
        return False
    
    async def verify_consensus(self, sequence_number: int, digest: str) -> bool:
        """Verify that consensus was reached for a specific sequence."""
        if sequence_number not in self.committed_certificates:
            return False
        
        commits = self.committed_certificates[sequence_number]
        return len(commits) >= 2 * self.f + 1


class GovernanceContract:
    """Smart contract-like governance for swarm decisions."""
    
    def __init__(self, registry: AgentRegistry, consensus: ConsensusEngine):
        self.registry = registry
        self.consensus = consensus
        self.proposals: Dict[str, Proposal] = {}
        self.execution_queue: asyncio.Queue = asyncio.Queue()
        self.callbacks: Dict[str, List[Callable]] = defaultdict(list)
        self._running = False
        
    async def create_proposal(
        self, 
        proposer_id: str, 
        title: str, 
        description: str, 
        action_type: str,
        payload: Dict[str, Any],
        voting_duration_hours: int = 24
    ) -> str:
        """Create new governance proposal."""
        # Verify proposer has sufficient stake
        if proposer_id not in self.registry.agents:
            raise ValueError("Agent not registered")
        
        agent = self.registry.agents[proposer_id]
        if agent.stake_amount < self.registry.min_stake * 2:
            raise InsufficientStake("Insufficient stake to create proposal")
        
        proposal_id = hashlib.sha256(
            f"{proposer_id}{time.time()}{title}".encode()
        ).hexdigest()[:16]
        
        proposal = Proposal(
            proposal_id=proposal_id,
            proposer_id=proposer_id,
            title=title,
            description=description,
            action_type=action_type,
            payload=payload,
            created_at=datetime.utcnow(),
            voting_end_time=datetime.utcnow() + timedelta(hours=voting_duration_hours),
            status=ProposalStatus.VOTING
        )
        
        # Use consensus to validate proposal creation
        consensus_data = {
            "type": "proposal_creation",
            "proposal_id": proposal_id,
            "proposer": proposer_id,
            "action": action_type
        }
        
        digest = await self.consensus.propose_value(consensus_data, proposer_id)
        if digest:
            self.proposals[proposal_id] = proposal
            logger.info(f"Proposal {proposal_id} created and accepted by consensus")
            return proposal_id
        else:
            raise QuorumNotReached("Failed to reach consensus on proposal creation")
    
    async def cast_vote(self, proposal_id: str, agent_id: str, approve: bool) -> bool:
        """Cast vote on a proposal with weighted voting power."""
        if proposal_id not in self.proposals:
            return False
        
        proposal = self.proposals[proposal_id]
        
        if proposal.status != ProposalStatus.VOTING:
            return False
        
        if datetime.utcnow() > proposal.voting_end_time:
            proposal.status = ProposalStatus.EXPIRED
            return False
        
        voting_power = self.registry.get_voting_power(agent_id)
        if voting_power <= 0:
            return False
        
        proposal.votes[agent_id] = approve
        proposal.vote_weights[agent_id] = voting_power
        
        # Check if quorum reached
        total_power = sum(
            self.registry.get_voting_power(aid) 
            for aid in self.registry.get_active_agents()
        )
        
        if proposal.total_voting_power / total_power >= proposal.required_quorum:
            await self._finalize_proposal(proposal_id)
        
        return True
    
    async def _finalize_proposal(self, proposal_id: str):
        """Finalize proposal based on votes."""
        proposal = self.proposals[proposal_id]
        
        if proposal.approval_power >= proposal.execution_threshold:
            proposal.status = ProposalStatus.PASSED
            await self.execution_queue.put(proposal)
            logger.info(f"Proposal {proposal_id} passed with {proposal.approval_power:.2%} approval")
        else:
            proposal.status = ProposalStatus.REJECTED
            logger.info(f"Proposal {proposal_id} rejected")
    
    async def execution_worker(self):
        """Background worker to execute passed proposals."""
        self._running = True
        while self._running:
            try:
                proposal = await asyncio.wait_for(
                    self.execution_queue.get(), 
                    timeout=1.0
                )
                await self._execute_proposal(proposal)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Execution error: {e}")
    
    async def _execute_proposal(self, proposal: Proposal):
        """Execute approved proposal actions."""
        try:
            # Trigger registered callbacks
            callbacks = self.callbacks.get(proposal.action_type, [])
            for callback in callbacks:
                await callback(proposal.payload)
            
            proposal.status = ProposalStatus.EXECUTED
            
            # Reward proposer and voters
            await self.registry.update_reputation(
                proposal.proposer_id, 0.5, proposal.proposal_id
            )
            
        except Exception as e:
            logger.error(f"Failed to execute proposal {proposal.proposal_id}: {e}")
            await self.registry.slash_agent(
                proposal.proposer_id, 
                f"Execution failed: {str(e)}",
                0.5
            )
    
    def register_callback(self, action_type: str, callback: Callable):
        """Register execution callback for action type."""
        self.callbacks[action_type].append(callback)


class SwarmOrchestrator:
    """Main orchestrator coordinating agents, tasks, and governance."""
    
    def __init__(self, f_tolerance: int = 1):
        self.registry = AgentRegistry()
        self.consensus = ConsensusEngine(self.registry, f_tolerance)
        self.governance = GovernanceContract(self.registry, self.consensus)
        self.task_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self.active_tasks: Dict[str, SwarmTask] = {}
        self.metrics: Dict[str, Any] = defaultdict(lambda: defaultdict(int))
        self._shutdown_event = asyncio.Event()
        
    async def submit_task(
        self, 
        task_type: str, 
        payload: Dict[str, Any], 
        complexity: float = 0.5,
        priority: int = 1,
        deadline_hours: Optional[int] = None
    ) -> str:
        """Submit task to swarm with governance oversight."""
        task_id = secrets.token_hex(8)
        
        deadline = None
        if deadline_hours:
            deadline = datetime.utcnow() + timedelta(hours=deadline_hours)
        
        task = SwarmTask(
            task_id=task_id,
            task_type=task_type,
            complexity=complexity,
            priority=priority,
            payload=payload,
            deadline=deadline
        )
        
        # High complexity tasks require governance approval
        if complexity > 0.8:
            proposal_id = await self.governance.create_proposal(
                proposer_id="system",
                title=f"Execute high-complexity task: {task_type}",
                description=f"Task {task_id} requires swarm approval due to complexity {complexity}",
                action_type="task_execution",
                payload={"task_id": task_id, "task": asdict(task)},
                voting_duration_hours=1
            )
            logger.info(f"High complexity task {task_id} submitted for governance approval: {proposal_id}")
        else:
            await self.task_queue.put((-priority, time.time(), task))
            self.active_tasks[task_id] = task
            logger.info(f"Task {task_id} queued with priority {priority}")
        
        return task_id
    
    async def task_dispatcher(self):
        """Dispatch tasks to appropriate agents based on reputation and load."""
        while not self._shutdown_event.is_set():
            try:
                _, _, task = await asyncio.wait_for(
                    self.task_queue.get(), 
                    timeout=2.0
                )
                
                # Select optimal agent using reputation-weighted random selection
                agent_id = await self._select_agent_for_task(task)
                
                if agent_id:
                    task.assigned_agent = agent_id
                    task.status = "assigned"
                    
                    # Create assignment proposal for transparency
                    await self.governance.create_proposal(
                        proposer_id="orchestrator",
                        title=f"Assign task {task.task_id} to {agent_id}",
                        description="Task assignment",
                        action_type="task_assignment",
                        payload={
                            "task_id": task.task_id,
                            "agent_id": agent_id,
                            "reward": task.calculate_reward()
                        },
                        voting_duration_hours=0  # Immediate for simple assignments
                    )
                else:
                    # Re-queue if no agent available
                    await self.task_queue.put((-task.priority, time.time(), task))
                    
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"Dispatcher error: {e}")
    
    async def _select_agent_for_task(self, task: SwarmTask) -> Optional[str]:
        """Select best agent for task using reputation-weighted algorithm."""
        active_agents = self.registry.get_active_agents()
        
        if not active_agents:
            return None
        
        # Calculate weights based on reputation and available stake
        weights = []
        for aid in active_agents:
            agent = self.registry.agents[aid]
            # Higher reputation = higher chance, but also consider stake
            weight = agent.reputation_score * (agent.stake_amount / 100)
            weights.append((aid, weight))
        
        # Weighted random selection
        total_weight = sum(w for _, w in weights)
        if total_weight == 0:
            return None
        
        import random
        r = random.uniform(0, total_weight)
        cumulative = 0
        
        for aid, weight in weights:
            cumulative += weight
            if r <= cumulative:
                return aid
        
        return active_agents[0] if active_agents else None
    
    async def report_task_completion(
        self, 
        task_id: str, 
        agent_id: str, 
        success: bool, 
        result_hash: str
    ):
        """Report task completion with verification."""
        if task_id not in self.active_tasks:
            return
        
        task = self.active_tasks[task_id]
        
        if success:
            # Verify result through consensus if high value
            if task.calculate_reward() > 50:
                consensus_result = await self.consensus.propose_value(
                    {
                        "task_id": task_id,
                        "result_hash": result_hash,
                        "agent_id": agent_id
                    },
                    agent_id
                )
                if not consensus_result:
                    success = False
                    logger.warning(f"Task {task_id} consensus verification failed")
        
        if success:
            reward = task.calculate_reward()
            await self.registry.update_reputation(agent_id, 0.1 * task.complexity, task_id)
            self.metrics[agent_id]["tasks_completed"] += 1
            self.metrics[agent_id]["total_rewards"] += reward
            task.status = "completed"
        else:
            await self.registry.slash_agent(agent_id, f"Task {task_id} failed", 0.5)
            task.status = "failed"
            # Re-queue for retry
            task.assigned_agent = None
            await self.task_queue.put((-task.priority, time.time(), task))
    
    async def detect_byzantine_agents(self):
        """Periodic check for Byzantine behavior patterns."""
        while not self._shutdown_event.is_set():
            await asyncio.sleep(60)  # Check every minute
            
            for agent_id in self.registry.get_active_agents():
                metrics = self.metrics[agent_id]
                completed = metrics.get("tasks_completed", 0)
                failed = metrics.get("tasks_failed", 0)
                
                if completed + failed > 10:  # Minimum sample size
                    failure_rate = failed / (completed + failed)
                    if failure_rate > 0.5:  # High failure rate
                        await self.registry.slash_agent(
                            agent_id, 
                            f"High failure rate detected: {failure_rate:.2%}",
                            3.0
                        )
                        self.registry.status[agent_id] = AgentStatus.BYZANTINE_DETECTED
    
    async def start(self):
        """Start orchestrator services."""
        logger.info("Starting SwarmOrchestrator...")
        
        # Start background tasks
        await asyncio.gather(
            self.governance.execution_worker(),
            self.task_dispatcher(),
            self.detect_byzantine_agents(),
            return_exceptions=True
        )
    
    async def shutdown(self):
        """Graceful shutdown."""
        logger.info("Shutting down SwarmOrchestrator...")
        self._shutdown_event.set()


# Example usage and integration
async def main():
    """Example initialization of DevFlowAI swarm."""
    orchestrator = SwarmOrchestrator(f_tolerance=1)
    
    # Register some agents
    for i in range(5):
        agent = AgentIdentity(
            agent_id=f"agent_{i}",
            public_key=f"pk_{i}_{secrets.token_hex(16)}",
            stake_amount=150.0 + (i * 50),
            reputation_score=2.0 + (i * 0.5)
        )
        await orchestrator.registry.register_agent(agent)
    
    # Register governance callback for task execution
    async def execute_task_payload(payload):
        logger.info(f"Executing task payload: {payload}")
        # Integration with actual execution logic
        pass
    
    orchestrator.governance.register_callback("task_execution", execute_task_payload)
    
    # Start services
    try:
        await orchestrator.start()
    except KeyboardInterrupt:
        await orchestrator.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
