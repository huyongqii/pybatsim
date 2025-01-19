#!/usr/bin/env python3

"""
A monitored Easy Backfill scheduler that inherits from the original EasyBackfill.
"""

from easyBackfill import EasyBackfill
import pandas as pd
from datetime import datetime

class ResourceMonitor:
    """资源使用监控器"""
    
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.usage_history = {}
        
    def update_resource_usage(self, job=None):
        """更新资源使用情况"""
        current_time = self.scheduler.bs.time()
        current_usage = {}
        
        # 记录作业信息
        job_info = None
        if job:
            job_info = {
                'job_id': job.id,
                'requested_resources': job.requested_resources,
                'requested_time': job.requested_time
            }
        
        # 获取当前资源使用情况
        total_resources = self.scheduler.bs.nb_resources
        used_resources = 0
        for running_job in self.scheduler.listRunningJob:
            used_resources += running_job.requested_resources
            
        # 计算资源利用率
        utilization = used_resources / total_resources if total_resources > 0 else 0
        
        # 记录当前时间点的系统状态
        self.usage_history[current_time] = {
            'total_resources': total_resources,
            'used_resources': used_resources,
            'utilization': utilization,
            'job_info': job_info,
            'waiting_jobs': len(self.scheduler.listWaitingJob)
        }
        
    def save_usage_history(self, output_file: str):
        """保存资源使用历史"""
        if not self.usage_history:
            print("Warning: No usage history to save")
            return
            
        records = []
        for timestamp, usage in self.usage_history.items():
            record = {
                'timestamp': timestamp,
                'total_resources': usage['total_resources'],
                'used_resources': usage['used_resources'],
                'utilization': usage['utilization'],
                'waiting_jobs': usage['waiting_jobs']
            }
            
            # 添加作业信息（如果有）
            if usage['job_info']:
                record.update({
                    'job_id': usage['job_info']['job_id'],
                    'requested_resources': usage['job_info']['requested_resources'],
                    'requested_time': usage['job_info']['requested_time']
                })
                
            records.append(record)
                
        df = pd.DataFrame(records)
        df.to_csv(output_file, index=False)
        print(f"Resource usage history saved to: {output_file}")

class M_easybackfill(EasyBackfill):
    """带资源监控的Easy Backfill调度器"""
    
    def onAfterBatsimInit(self):
        """初始化时添加监控器"""
        super().onAfterBatsimInit()
        self.monitor = ResourceMonitor(self)
        
    def onJobSubmission(self, job):
        """作业提交时记录资源状态"""
        super().onJobSubmission(job)
        self.monitor.update_resource_usage(job)
        
    def onJobCompletion(self, job):
        """作业完成时记录资源状态"""
        super().onJobCompletion(job)
        self.monitor.update_resource_usage(job)
        
    def onSimulationEnds(self):
        """模拟结束时保存监控数据"""
        super().onSimulationEnds()
        self.monitor.save_usage_history('/root/PredictModel/hpc_simulation/result/resource_usage.csv')
        