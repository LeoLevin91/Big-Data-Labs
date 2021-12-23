import logging
logging.basicConfig()
from time import time, sleep
from threading import Thread
from kazoo.client import KazooClient as KazCli


class Philosopher(Thread):
    def __init__(self, nodeOut, idOut, pathToForksOut, lunchTimeOut = 2, mindTimeOut = 1, durationOut = 50):
        super().__init__()
        self.node = nodeOut                                   
        self.pathToForks = pathToForksOut                 
        self.id = idOut                                      
        self.leftForkId = idOut                             
        self.rightForkId = idOut + 1 if idOut + 1 < 6 else 1 
        self.duration = durationOut                           
        self.lunchTime = lunchTimeOut                           
        self.mindTime = mindTimeOut                       

    def run(self):
        kc = KazCli()
        kc.start()

        table = kc.Lock(f'{self.node}/table', self.id)
        leftFork = kc.Lock(f'{self.node}/{self.pathToForks}/{self.leftForkId}', self.id)
        rightFork = kc.Lock(f'{self.node}/{self.pathToForks}/{self.rightForkId}', self.id)

        begin = time()
        print(f'The philosopher {self.id} thinking.')
        while time() - begin < self.duration:          
            with table:
                if len(leftFork.contenders()) == 0 and len(rightFork.contenders()) == 0:
                    leftFork.acquire()
                    rightFork.acquire()
            
            if leftFork.is_acquired:
                print(f'The philosopher {self.id} eating.')
                sleep(self.lunchTime)
                leftFork.release()
                rightFork.release()
                
                print(f'The philosopher {self.id} thinking.')
                sleep(self.mindTime)
                        
        kc.stop()
        kc.close()


if __name__ == "__main__":
    mainKazCli = KazCli()
    mainKazCli.start()

    node = '/PhilosophersLunch'
    pathToForks = '/forks'
    
    if mainKazCli.exists(node):
        mainKazCli.delete(node, recursive=True)
        
    mainKazCli.create('/PhilosophersLunch')
    mainKazCli.create('/PhilosophersLunch/table')
    mainKazCli.create('/PhilosophersLunch/forks')
    mainKazCli.create('/PhilosophersLunch/forks/1')
    mainKazCli.create('/PhilosophersLunch/forks/2')
    mainKazCli.create('/PhilosophersLunch/forks/3')
    mainKazCli.create('/PhilosophersLunch/forks/4')
    mainKazCli.create('/PhilosophersLunch/forks/5')

    for i in range(5):
        p = Philosopher(node, i, pathToForks, lunchTimeOut=4, mindTimeOut = 1, durationOut=40)
        p.start()

