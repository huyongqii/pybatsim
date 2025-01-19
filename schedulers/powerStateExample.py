from batsim.batsim import BatsimScheduler
from procset import ProcSet

class PowerStateExample(BatsimScheduler):
    def __init__(self):
        super().__init__()
        self.sleep_time = 100  # 设置睡眠时间
        
    def onSimulationBegins(self):
        print("Simulation starts")
        self.wake_me_later(self.sleep_time)
        
    def onRequestedCall(self):
        # 获取所有空闲节点
        idle_resources = self.platform.get_idle_resources()
        if idle_resources:
            # 将空闲节点设置为睡眠状态
            self.set_resource_state(idle_resources, "sleeping")
            
    def onResourceStateChanged(self, resources, state):
        print(f"Resources {resources} changed to state: {state}")
        
    def wake_up_resources(self, resources):
        # 唤醒睡眠中的节点
        self.set_resource_state(resources, "idle") 