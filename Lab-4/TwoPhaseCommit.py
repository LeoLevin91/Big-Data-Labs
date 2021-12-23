import logging
logging.basicConfig()

from threading import Thread, Timer
from time import sleep, time

from numpy.random import random

from kazoo.client import KazooClient as KClient


class Participant(Thread):
    def __init__(self, path, _id, decisionTimeOut=4, trueProbabilityOut=0.5):
        super().__init__()
        self.node = f'{path}/{_id}'
        self.id = _id
        self.trueProbability = trueProbabilityOut
        self.decisionTime = decisionTimeOut

    def run(self):
        participant = KClient()
        participant.start()
        
        sleep(self.decisionTime)
        
        decision = b'commit' if random() < self.trueProbability else b'abort'
        print(f'Voting №{self.id}: My desition... {decision.decode()}')
        participant.create(self.node, decision, ephemeral=True)
        
        @participant.DataWatch(self.node)
        def watch_myself(data, stat):
            if stat.version != 0:
                print(f'Voting №{self.id}: Received a response from the coordinator: {data.decode()}')

        sleep(10)
        
        participant.stop()
        participant.close()


class Coordinator():
    def main(self, root, numPartisipantsOut=5, durationOut=30):
        coordinator = KClient()
        coordinator.start()

        if coordinator.exists(root):
            coordinator.delete(root, recursive=True)

        coordinator.create(root)
        partisipantsNode = f'{root}/partisipants'
        coordinator.create(partisipantsNode)

        self.timer = None

        def makeMainDecision():
            partisipants = coordinator.get_children(partisipantsNode)
            decisions = []
            for partisipant in partisipants:
                decisions.append(coordinator.get(f'{partisipantsNode}/{partisipant}')[0] == b'commit')

            mainDecision = b'commit' if all(decisions) else b'abort'
            for partisipant in partisipants:
                coordinator.set(f'{partisipantsNode}/{partisipant}', mainDecision)

        @coordinator.ChildrenWatch(partisipantsNode)
        def watchPartisipants(partisipants):
            if self.timer is not None:
                self.timer.cancel()
            if len(partisipants) != 0:
                self.timer = Timer(duration, makeMainDecision)
                self.timer.daemon = True
                self.timer.start()

            if len(partisipants) == numPartisipantsOut:
                print('Coordinator: Voicing the main decision...')
                self.timer.cancel()
                makeMainDecision()

        for i in range(numPartisipantsOut):
            p = Participant(partisipantsNode, i, decisionTimeOut=3, trueProbabilityOut=0.6)
            p.start()


if __name__ == "__main__":
    root = '/coordinator'
    duration = 30
    Coordinator().main(root, durationOut=duration)
