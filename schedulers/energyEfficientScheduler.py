from batsim.batsim import BatsimScheduler
from procset import ProcSet
from enum import Enum
from batsim.sched import Job
from batsim.sched import Scheduler
from batsim.sched import Allocation
from batsim.sched import Profiles

class PState(Enum):
    ON = 0                # 开机状态
    OFF = 1              # 关机状态
    SWITCHING_OFF = 2     # 正在关机
    SWITCHING_ON = 3    # 正在开机

class NodePowerController:
    def __init__(self, batsim_scheduler):
        self.bs = batsim_scheduler
        
    def switch_on_nodes(self, start_node, end_node):
        """开启指定范围的节点"""
        self.bs.set_resource_state(f"{start_node}-{end_node}", str(PState.ON.value))
        
    def switch_off_nodes(self, start_node, end_node):
        """关闭指定范围的节点"""
        self.bs.set_resource_state(f"{start_node}-{end_node}", str(PState.OFF.value))

class EnergyEfficientScheduler(BatsimScheduler):
    def __init__(self, options=None):
        super().__init__(options)
        # 能源和电源管理参数
        self.idle_timeout = 120       # 节点空闲超时时间（秒）
        self.idle_times = {}          # 记录节点空闲时间
        self.low_util_threshold = 0.3  # 低利用率阈值
        self.high_util_threshold = 0.7 # 高利用率阈值
        
        # 能源统计
        self.power_idle = 250.0      # 空闲功率（瓦特）
        self.power_compute = 800.0    # 计算功率（瓦特）
        self.energy_saved = 0.0       # 节省的能源（瓦特时）
        self.last_check_time = 0      # 上次检查时间
        
        # 移除这里的初始化
        self.power_controller = None  # 将在 onAfterBatsimInit 中初始化
        
        # 添加节点状态跟踪
        self.node_states = {}  # 记录每个节点的状态
        
        # 添加日志文件
        self.state_log = None
        
        # CPU 相关配置
        self.cores_per_node = 56      # 每个节点的核心数
        self.gf_per_core = 40*10e12         # 每个核心的计算能力(GF)
        self.node_core_usage = {}     # 记录每个节点已使用的核心数
        
        # 作业状态管理
        self.waiting_jobs = []    # 等待调度的作业
        self.running_jobs = []    # 正在运行的作业
        self.completed_jobs = []  # 已完成的作业
        
    def onAfterBatsimInit(self):
        super().onAfterBatsimInit()
        self.power_controller = NodePowerController(self.bs)
        
        # 初始化所有节点状态和CPU使用情况
        for resource in range(self.bs.nb_resources):
            self.node_states[resource] = PState.ON
            self.node_core_usage[resource] = 0  # 初始化CPU使用为0

    def wake_me_later(self, delay):
        """设置延迟回调，用于定期检查集群状态"""
        next_time = self.bs.time() + delay
        self.bs.wake_me_up_at(next_time) 

    def onSimulationBegins(self):
        super().onSimulationBegins()
        print("Energy Efficient EASY-Backfill Scheduler Started")
        
        # 初始化节点状态日志
        self.state_log = open("node_states.csv", "w")
        self.state_log.write("time,node_id,old_state,new_state,event\n")
        
        # 初始化节点空闲时间记录
        for resource in range(self.bs.nb_resources):
            self.idle_times[resource] = 0
            
        # 设置定期检查
        self.last_check_time = self.bs.time()
        self.wake_me_later(self.idle_timeout)
        
    def filter_jobs_by_state(self, state):
        """按状态过滤作业"""
        # return [job for job in self.bs.jobs.values() if job.job_state == state]
        if state == Job.State.SUBMITTED:
            return self.waiting_jobs
        elif state == Job.State.RUNNING:
            return self.running_jobs
        elif state == Job.State.COMPLETED_SUCCESSFULLY:
            return self.completed_jobs
        return []

    def check_cluster_status(self):
        """检查集群状态"""
        current_time = self.bs.time()
        
        # 获取各种节点状态的数量
        total_nodes = self.bs.nb_resources
        idle_nodes = len(self.get_idle_resources())
        sleeping_nodes = len(self.get_sleeping_resources())
        switching_on_nodes = len([r for r in range(self.bs.nb_resources) 
                                if self.node_states[r] == PState.SWITCHING_ON])
        switching_off_nodes = len([r for r in range(self.bs.nb_resources) 
                                 if self.node_states[r] == PState.SWITCHING_OFF])
        
        # 计算运行中的节点数和计算中的节点数
        running_nodes = total_nodes - sleeping_nodes - switching_off_nodes  # 所有可用节点
        computing_nodes = 0  # 实际在计算的节点数
        
        # 统计正在计算的节点
        running_jobs = self.filter_jobs_by_state(Job.State.RUNNING)
        allocated_nodes = set()
        for job in running_jobs:
            allocated_nodes.update(job.allocation)
        computing_nodes = len(allocated_nodes)
        
        # 计算真实利用率
        utilization = computing_nodes / running_nodes if running_nodes > 0 else 0
        
        # 计算节省的能源
        time_elapsed = current_time - self.last_check_time
        power_saved = sleeping_nodes * (self.power_idle - 0)
        self.energy_saved += power_saved * time_elapsed / 3600
        
        self.last_check_time = current_time
        
        return {
            "utilization": utilization,
            "running_nodes": running_nodes,
            "computing_nodes": computing_nodes,
            "idle_nodes": idle_nodes,
            "sleeping_nodes": sleeping_nodes,
            "switching_on_nodes": switching_on_nodes,
            "switching_off_nodes": switching_off_nodes,
            "total_nodes": total_nodes,
            "waiting_jobs": len(self.filter_jobs_by_state(Job.State.SUBMITTED))
        }
        
    def get_idle_resources(self):
        """获取空闲资源"""
        running_jobs = self.filter_jobs_by_state(Job.State.RUNNING)
        allocated_nodes = set()
        for job in running_jobs:
            allocated_nodes.update(job.allocation)
            
        return [r for r in range(self.bs.nb_resources) 
                if self.node_states[r] == PState.ON  # 只返回完全开机的节点
                and r not in allocated_nodes]
                
    def get_sleeping_resources(self):
        """获取睡眠资源"""
        return [r for r in range(self.bs.nb_resources) 
                if self.node_states[r] == PState.OFF]  # 只返回完全关机的节点
        
    def log_state_change(self, node_id, old_state, new_state, event):
        """记录节点状态变化
        参数:
        - node_id: 节点ID
        - old_state: 原状态
        - new_state: 新状态
        - event: 触发事件（如 'power_off', 'power_on', 'complete_on', 'complete_off'）
        """
        if self.state_log:
            self.state_log.write(f"{self.bs.time()},{node_id},{old_state},{new_state},{event}\n")
            self.state_log.flush()  # 立即写入文件

    def manage_power_states(self):
        """管理节点电源状态"""
        status = self.check_cluster_status()
        current_time = self.bs.time()
        
        # 低负载时关闭节点
        if status["utilization"] < self.low_util_threshold and status["waiting_jobs"] <= 1:
            idle_resources = self.get_idle_resources()
            min_active_nodes = max(2, int(self.bs.nb_resources * 0.3))
            if len(idle_resources) > min_active_nodes:
                to_sleep = []
                for resource in idle_resources[min_active_nodes:]:
                    if (self.node_states[resource] == PState.ON and 
                        current_time - self.idle_times[resource] > self.idle_timeout):
                        to_sleep.append(resource)
                        
                if to_sleep:
                    print(f"Shutting down nodes {to_sleep} to save energy")
                    print(f"Current energy saved: {self.energy_saved:.2f} Wh")
                    for node in to_sleep:
                        if self.node_states[node] == PState.ON:
                            self.power_controller.switch_off_nodes(node, node)
                            old_state = self.node_states[node]
                            self.node_states[node] = PState.SWITCHING_OFF
                            self.log_state_change(node, old_state, PState.SWITCHING_OFF, "power_off")
        
        # 高负载时开启节点
        elif (status["utilization"] > self.high_util_threshold or 
              status["waiting_jobs"] > 0):
            sleeping_resources = self.get_sleeping_resources()
            if sleeping_resources:
                needed_nodes = max(
                    status["waiting_jobs"] - status["idle_nodes"],
                    1
                )
                to_wake = sleeping_resources[:needed_nodes]
                if to_wake:
                    print(f"Waking up nodes {to_wake} due to high load")
                    for node in to_wake:
                        if self.node_states[node] == PState.OFF:
                            self.power_controller.switch_on_nodes(node, node)
                            self.node_states[node] = PState.SWITCHING_ON  # 设置为开机中状态
                            old_state = self.node_states[node]
                            self.node_states[node] = PState.SWITCHING_ON
                            self.log_state_change(node, old_state, PState.SWITCHING_ON, "power_on")
    
    def onJobCompletion(self, job):
        """作业完成时调用"""
        # 从运行队列移到完成队列
        if job in self.running_jobs:
            self.running_jobs.remove(job)
        self.completed_jobs.append(job)
        
        # 释放资源
        self.release_cores(job)
        
        # 更新节点空闲时间
        current_time = self.bs.time()
        for resource in job.allocation:
            self.idle_times[resource] = current_time
        
        # 尝试调度等待的作业
        self.scheduleJobs()
        self.manage_power_states()

    def onRequestedCall(self):
        """定期检查"""
        self.manage_power_states()
        # 设置下一次检查
        # self.wake_me_later(self.idle_timeout)
        
    def onNoMoreEvents(self):
        """当没有更多事件时被调用"""
        # 检查是否还有正在运行或等待的作业
        if not self.running_jobs and not self.waiting_jobs:
            print("All jobs completed, simulation can end")
            return
        
        # 如果还有作业，尝试调度它们
        if self.waiting_jobs:
            print(f"Still have {len(self.waiting_jobs)} jobs waiting")
            self.scheduleJobs()

    def onSimulationEnds(self):
        """模拟结束时调用"""
        # 检查是否有未完成的作业
        if self.waiting_jobs:
            print(f"Warning: {len(self.waiting_jobs)} jobs were not scheduled")
            for job in self.waiting_jobs:
                print(f"- Job {job.id}: requested {job.requested_resources} nodes, {self.get_job_core_request(job)} cores per node")
        
        if self.running_jobs:
            print(f"Warning: {len(self.running_jobs)} jobs were still running")
            for job in self.running_jobs:
                print(f"- Job {job.id} was running on nodes {job.allocation}")

        print(f"\nSimulation completed!")
        total_time_hours = self.bs.time() / 3600
        print(f"Total energy saved: {self.energy_saved:.2f} Wh")
        print(f"Average power saved: {self.energy_saved/total_time_hours:.2f} W")
        
        # 打印最终统计信息
        print("\nFinal statistics:")
        print(f"Total jobs completed: {len(self.completed_jobs)}")
        print(f"Jobs not scheduled: {len(self.waiting_jobs)}")
        print(f"Jobs still running: {len(self.running_jobs)}")
        
        # 关闭日志文件
        if self.state_log:
            self.state_log.close()

    def onJobSubmission(self, job):
        """新作业提交时调用"""
        if job.requested_resources > self.bs.nb_resources:
            self.bs.reject_jobs([job])
            return
            
        # 添加到等待队列
        self.waiting_jobs.append(job)
        
        # 调用调度算法
        self.scheduleJobs()
        
        # 检查是否需要唤醒节点
        self.manage_power_states()

    def onJobKilled(self, job_ids):
        """作业被杀死时调用
        参数:
        - job_ids: 被杀死的作业ID列表
        """
        # TODO: 实现作业被杀死的处理
        assert False

    # def onResourceStateChanged(self, resources, state):
    #     """当节点状态改变时更新我们的记录"""
    #     for r in resources:
    #         self.node_states[r] = state
    #     print(f"Resources {resources} changed to state: {state}")

    def onQuery(self, query):
        """响应查询请求
        参数:
        - query: 查询内容，可能包含:
          - estimate_waiting_time: 估计作业等待时间
          - consumed_energy: 查询能源消耗
        """
        # TODO: 实现查询响应
        pass

    def onMachinePStateChanged(self, machines, new_pstate):
        """处理节点电源状态变化"""
        for machine in machines:
            old_state = self.node_states[machine]
            new_pstate = int(new_pstate)
            
            # 根据实际状态更新中间态
            if new_pstate == PState.ON.value:
                if old_state == PState.SWITCHING_ON:
                    self.node_states[machine] = PState.ON
                    print(f"Node {machine} completed switching ON")
                    self.idle_times[machine] = self.bs.time()
                    self.log_state_change(machine, old_state, PState.ON, "complete_on")
            elif new_pstate == PState.OFF.value:
                if old_state == PState.SWITCHING_OFF:
                    self.node_states[machine] = PState.OFF
                    print(f"Node {machine} completed switching OFF")
                    self.log_state_change(machine, old_state, PState.OFF, "complete_off")

    def get_job_core_request(self, job):
        """计算作业需要的CPU核心数"""
        profile = self.bs.profiles[job.workload][job.profile]
        if profile["type"] != "parallel_homogeneous":
            return 0
        gf_request = float(profile["cpu"])  # 将"80GF"转换为80
        cores_needed = int(gf_request / self.gf_per_core)
        return max(1, cores_needed)  # 至少需要1个核心

    def get_available_nodes_with_cores(self, cores_needed):
        """获取有足够空闲核心的节点，支持计算共享"""
        available_nodes = []
        for node in range(self.bs.nb_resources):
            if (self.node_states[node] == PState.ON and  # 节点开机
                self.node_core_usage[node] + cores_needed <= self.cores_per_node):  # 有足够的核心
                available_nodes.append((node, self.node_core_usage[node]))  # 返回节点ID和当前使用的核心数
        # 按照核心使用量排序，优先使用已经在使用的节点
        return sorted(available_nodes, key=lambda x: (-x[1] if x[1] > 0 else float('inf')))

    def allocate_cores(self, job, nodes):
        """为作业分配CPU核心"""
        cores_per_node = self.get_job_core_request(job)
        for node in nodes:
            self.node_core_usage[node] += cores_per_node

    def release_cores(self, job):
        """释放作业占用的CPU核心"""
        cores_per_node = self.get_job_core_request(job)
        for node in job.allocation:
            self.node_core_usage[node] -= cores_per_node

    def scheduleJobs(self):
        """实现考虑CPU核心的作业调度，支持计算共享"""
        if not self.waiting_jobs:
            return
        
        # 创建等待队列的副本进行操作
        current_waiting_jobs = self.waiting_jobs.copy()
        cur_time = self.bs.time()
        # 按完成时间排序
        current_waiting_jobs.sort(key=lambda j: j.requested_time)  # 短作业优先
        
        # 存储可以调度的作业
        jobs_to_execute = []
        
        # 尝试为每个等待的作业分配资源
        for job in current_waiting_jobs:
            # 计算每个节点需要的核心数
            cores_needed = self.get_job_core_request(job)
            if cores_needed == 0:  # 无效的作业类型
                continue
                
            # 获取有足够核心的可用节点
            available_nodes = self.get_available_nodes_with_cores(cores_needed)
            
            # 检查是否有足够的节点
            if len(available_nodes) >= job.requested_resources:
                # 从等待队列中移除
                if job in self.waiting_jobs:
                    self.waiting_jobs.remove(job)
                
                # 分配节点给作业（选择核心使用最多的节点）
                selected_nodes = [node for node, usage in available_nodes[:job.requested_resources]]
                job.allocation = ProcSet(*selected_nodes)
                
                # 更新CPU使用情况
                self.allocate_cores(job, selected_nodes)
                
                # 添加到待执行列表和运行队列
                jobs_to_execute.append(job)
                self.running_jobs.append(job)
                
                print(f"Allocated nodes {job.allocation} for job {job.id} (cores per node: {cores_needed}, total cores: {cores_needed * job.requested_resources})")
            else:
                # 无法调度的作业重新加入等待队列
                self.waiting_jobs.append(job)
        
        # 批量执行所有可以调度的作业
        if jobs_to_execute:
            print(f"Executing batch of {len(jobs_to_execute)} jobs")
            self.bs.execute_jobs(jobs_to_execute)
            
            # 打印节点使用情况
            total_cores = self.cores_per_node * self.bs.nb_resources
            used_cores = sum(self.node_core_usage.values())
            print(f"Core usage: {used_cores}/{total_cores} ({used_cores/total_cores*100:.2f}%)")
