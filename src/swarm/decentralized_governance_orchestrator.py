import asyncio
from dataclasses import dataclass
from typing import List, Dict, Optional
from datetime import datetime
import random

@dataclass
class Reviewer:
    id: str
    expertise: List[str]
    current_workload: int
    availability: float  # 0.0 to 1.0
    last_review: datetime

@dataclass
class CodeReview:
    id: str
    files: List[str]
    complexity: float
    required_expertise: List[str]
    priority: int

class DecentralizedGovernanceOrchestrator:
    def __init__(self):
        self.reviewers: Dict[str, Reviewer] = {}
        self.pending_reviews: List[CodeReview] = []
        self.review_assignments: Dict[str, str] = {}  # review_id -> reviewer_id
        self.MAX_WORKLOAD = 5

    async def register_reviewer(self, reviewer: Reviewer) -> None:
        self.reviewers[reviewer.id] = reviewer

    async def submit_review(self, review: CodeReview) -> None:
        self.pending_reviews.append(review)
        await self._process_review_queue()

    def calculate_reviewer_score(self, reviewer: Reviewer, review: CodeReview) -> float:
        # Calculate match score between reviewer and review
        expertise_match = len(set(reviewer.expertise) & set(review.required_expertise))
        workload_factor = 1.0 - (reviewer.current_workload / self.MAX_WORKLOAD)
        time_since_last_review = (datetime.now() - reviewer.last_review).total_seconds() / 3600
        time_factor = min(1.0, time_since_last_review / 24)

        return (
            expertise_match * 0.5 +
            workload_factor * 0.3 +
            reviewer.availability * 0.1 +
            time_factor * 0.1
        )

    async def _process_review_queue(self) -> None:
        if not self.pending_reviews:
            return

        # Sort reviews by priority
        self.pending_reviews.sort(key=lambda x: x.priority, reverse=True)

        for review in self.pending_reviews[:]:
            best_reviewer = None
            best_score = -1

            for reviewer in self.reviewers.values():
                if reviewer.current_workload >= self.MAX_WORKLOAD:
                    continue

                score = self.calculate_reviewer_score(reviewer, review)
                if score > best_score:
                    best_score = score
                    best_reviewer = reviewer

            if best_reviewer and best_score > 0.3:  # Minimum score threshold
                best_reviewer.current_workload += 1
                best_reviewer.last_review = datetime.now()
                self.review_assignments[review.id] = best_reviewer.id
                self.pending_reviews.remove(review)
                await self._notify_reviewer(best_reviewer.id, review)

    async def _notify_reviewer(self, reviewer_id: str, review: CodeReview) -> None:
        # Placeholder for notification system
        print(f"Assigned review {review.id} to reviewer {reviewer_id}")

    async def complete_review(self, review_id: str, reviewer_id: str) -> None:
        if review_id in self.review_assignments:
            if self.review_assignments[review_id] == reviewer_id:
                self.reviewers[reviewer_id].current_workload -= 1
                del self.review_assignments[review_id]
                await self._process_review_queue()

    async def get_reviewer_stats(self) -> Dict[str, Dict]:
        return {
            reviewer_id: {
                'current_workload': reviewer.current_workload,
                'availability': reviewer.availability,
                'expertise': reviewer.expertise
            }
            for reviewer_id, reviewer in self.reviewers.items()
        }

    async def rebalance_workload(self) -> None:
        # Periodically rebalance workload across reviewers
        overloaded = [r for r in self.reviewers.values() if r.current_workload > (self.MAX_WORKLOAD * 0.8)]
        underloaded = [r for r in self.reviewers.values() if r.current_workload < (self.MAX_WORKLOAD * 0.2)]

        for over_reviewer in overloaded:
            for review_id, reviewer_id in self.review_assignments.items():
                if reviewer_id == over_reviewer.id:
                    for under_reviewer in underloaded:
                        if self.calculate_reviewer_score(under_reviewer, self.pending_reviews[0]) > 0.5:
                            self.review_assignments[review_id] = under_reviewer.id
                            over_reviewer.current_workload -= 1
                            under_reviewer.current_workload += 1
                            break