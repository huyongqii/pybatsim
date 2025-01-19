from batsim.batsim import BatsimScheduler
from procset import ProcSet

class PowerAwareScheduler(BatsimScheduler):
    def __init__(self):
        super().__init__()
        self.idle_timeout = 300  # 空闲超时时间（秒）
        self.idle_times = {}    # 记录节点空闲时间
        
    def onSimulationBegins(self):
        # 初始化所有节点的空闲时间记录
        for resource in range(self.platform.get_nb_resources()):
            self.idle_times[resource] = 0
            
    def onJobSubmission(self, job):
        # 需要的资源数量
        needed_resources = job.requested_resources
        
        # 获取空闲节点
        idle_resources = self.platform.get_idle_resources()
        
        # 如果空闲节点不够，尝试唤醒睡眠节点
        if len(idle_resources) < needed_resources:
            sleeping_resources = self.platform.get_resources_in_state("sleeping")
            to_wake = list(sleeping_resources)[:needed_resources - len(idle_resources)]
            if to_wake:
                self.set_resource_state(to_wake, "idle")
                
        # 分配作业
        if len(idle_resources) >= needed_resources:
            allocated_resources = list(idle_resources)[:needed_resources]
            self.execute_job(job.id, allocated_resources)
            
    def onJobCompletion(self, job):
        # 记录节点变为空闲的时间
        for resource in job.allocation:
            self.idle_times[resource] = self.time()
            
        # 检查是否有等待的作业可以执行
        self.schedule_waiting_jobs()
            
    def onRequestedCall(self):
        # 检查空闲节点是否超时
        current_time = self.time()
        idle_resources = self.platform.get_idle_resources()
        
        to_sleep = []
        for resource in idle_resources:
            if current_time - self.idle_times[resource] > self.idle_timeout:
                to_sleep.append(resource)
                
        if to_sleep:
            self.set_resource_state(to_sleep, "sleeping")
            
    def schedule_waiting_jobs(self):
        # 实现等待作业的调度逻辑
        pass 