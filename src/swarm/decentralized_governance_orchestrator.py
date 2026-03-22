import asyncio
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum
import hashlib
import time

class ConsensusState(Enum):
    PROPOSED = 'PROPOSED'
    VALIDATING = 'VALIDATING'
    COMMITTED = 'COMMITTED'
    REJECTED = 'REJECTED'

@dataclass
class ProposalBlock:
    id: str
    timestamp: float
    data: Dict
    previous_hash: str
    proposer: str
    signatures: List[str] = None
    state: ConsensusState = ConsensusState.PROPOSED

class DecentralizedGovernanceOrchestrator:
    def __init__(self, node_id: str, min_validators: int = 3):
        self.node_id = node_id
        self.min_validators = min_validators
        self.proposals: Dict[str, ProposalBlock] = {}
        self.chain: List[ProposalBlock] = []
        self.peers: Dict[str, 'Node'] = {}
        self.validation_threshold = 0.67  # 2/3 majority

    async def propose_change(self, data: Dict) -> str:
        """Propose a new change to the network"""
        block = ProposalBlock(
            id=self._generate_id(),
            timestamp=time.time(),
            data=data,
            previous_hash=self._get_last_hash(),
            proposer=self.node_id,
            signatures=[]
        )
        
        self.proposals[block.id] = block
        await self._broadcast_proposal(block)
        return block.id

    async def validate_proposal(self, proposal_id: str, validator_id: str) -> bool:
        """Validate a proposal using adaptive validation rules"""
        if proposal_id not in self.proposals:
            return False

        block = self.proposals[proposal_id]
        
        # Implement adaptive validation based on proposal type
        is_valid = await self._run_validation_checks(block)
        
        if is_valid:
            block.signatures.append(validator_id)
            
            # Check if we have enough signatures
            if len(block.signatures) >= self._calculate_required_validators():
                await self._commit_proposal(block)
                
        return is_valid

    async def _commit_proposal(self, block: ProposalBlock) -> None:
        """Commit a validated proposal to the chain"""
        block.state = ConsensusState.COMMITTED
        self.chain.append(block)
        await self._broadcast_commit(block)

    def _calculate_required_validators(self) -> int:
        """Dynamically calculate required validators based on network size"""
        total_peers = len(self.peers)
        return max(self.min_validators, int(total_peers * self.validation_threshold))

    async def _run_validation_checks(self, block: ProposalBlock) -> bool:
        """Run comprehensive validation checks on a proposal"""
        if not self._verify_hash_chain(block):
            return False

        if not self._verify_timestamps(block):
            return False

        return await self._verify_proposal_data(block)

    def _verify_hash_chain(self, block: ProposalBlock) -> bool:
        """Verify the integrity of the hash chain"""
        if not self.chain:  # Genesis block
            return block.previous_hash == '0' * 64

        return block.previous_hash == self._get_last_hash()

    def _verify_timestamps(self, block: ProposalBlock) -> bool:
        """Verify temporal consistency"""
        if not self.chain:
            return True

        return block.timestamp > self.chain[-1].timestamp

    async def _verify_proposal_data(self, block: ProposalBlock) -> bool:
        """Verify proposal data integrity and compliance"""
        try:
            # Add custom validation logic here based on proposal type
            return True
        except Exception:
            return False

    def _get_last_hash(self) -> str:
        """Get hash of the last block in the chain"""
        if not self.chain:
            return '0' * 64
        
        last_block = self.chain[-1]
        return self._calculate_hash(last_block)

    def _calculate_hash(self, block: ProposalBlock) -> str:
        """Calculate cryptographic hash of a block"""
        block_data = f"{block.id}{block.timestamp}{block.data}{block.previous_hash}{block.proposer}"
        return hashlib.sha256(block_data.encode()).hexdigest()

    def _generate_id(self) -> str:
        """Generate unique proposal ID"""
        return hashlib.sha256(f"{self.node_id}{time.time()}".encode()).hexdigest()[:12]

    async def _broadcast_proposal(self, block: ProposalBlock) -> None:
        """Broadcast new proposal to all peers"""
        for peer in self.peers.values():
            await peer.receive_proposal(block)

    async def _broadcast_commit(self, block: ProposalBlock) -> None:
        """Broadcast committed proposal to all peers"""
        for peer in self.peers.values():
            await peer.receive_commit(block)

    def get_chain_status(self) -> Dict:
        """Get current status of the governance chain"""
        return {
            'chain_length': len(self.chain),
            'pending_proposals': len(self.proposals),
            'last_hash': self._get_last_hash(),
            'active_peers': len(self.peers)
        }
