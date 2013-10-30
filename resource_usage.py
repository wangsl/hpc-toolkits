#!/bin/env python

# $Id$

# ps -o pid,ppid,ruser,pcpu,rss,vsz,cmd --user sw77
# ps -o pid,ppid,ruser,pcpu,rss,vsz,cmd --ppid 4530

import subprocess, os, sys
from subprocess import Popen as popen
from os import getenv, getpid, environ
from time import sleep
from sys import argv, exit
from socket import gethostname
from string import atoi
from datetime import datetime

def die(information) :
    print information
    sys.exit(1)
    return

def usage() :
    environment_variables = [
        'TOTAL_WALLTIME_TO_MONITOR_RESOURCE_USAGE',
        'PERIOD_TO_MONITOR_RESOURCE_USAGE',
        'COMMAND_TO_RUN',
        'RESOURCE_USAGE_LOG_FILE'
        ]
    
    print
    print " Usage: " + argv[0] + " -run : to run jobs"
    print "        " + argv[0] + " -help : print this help information"
    print
    
    for env in environment_variables :
        print " export " + env + "="
    print
    exit()
    

def list_uniq(alist) :
    '''
    Fastest order preserving
    '''
    set = {}
    return [set.setdefault(e,e) for e in alist if e not in set]

def current_time() :
    return datetime.now().strftime('%Y-%m-%d-%H:%M:%S')

def proc_status(pid):
    proc = "/proc/%d/status" % pid
    if os.path.exists(proc) :
        for line in open(proc).readlines() :
            if line.startswith("State:") :
                return line.split(":",1)[1].strip().split(' ')[0]
    return None

def _subprocess_from(ppid) :
    assert ppid
    ps_command = popen('ps --no-headers -o pid --ppid %d 2>&1' % ppid,
                       shell=True, stdout=subprocess.PIPE)
    ps_output = ps_command.stdout.read()
    if ps_command.wait() : return []

    sub_processes = []
    for ps in ps_output.split() :
        sub_processes.append(int(ps))

    return sub_processes

def _get_all_processes_from(ppid) :
    assert ppid
    subprocesses = [ppid]
    child_subprocesses = [ppid]
    while 1 :
        new_child_subprocesses = []
        for pid in child_subprocesses :
            new_child_subprocesses += _subprocess_from(pid)
        if len(new_child_subprocesses) == 0 : break
        child_subprocesses = new_child_subprocesses
        subprocesses += child_subprocesses
        
    return subprocesses 

class PSProcess :

    def __init__(self, pid) :
        self.pid = int(pid)
        self.ppid = None
        self.ruser = None
        self.pcpu = 0.0
        self.rss = 0.0
        self.vsz = 0.0
        self.cmd = None
        self._setup_from_ps()
        return

    def _setup_from_ps(self) :
        assert self.pid
        if not proc_status(self.pid) : return
        
        ps_command = popen("ps --no-headers -o pid,ppid,ruser,pcpu,rss,vsz,cmd --pid %d 2>&1" % self.pid,
                           shell=True, stdout=subprocess.PIPE)
        ps_output = ps_command.stdout.read()
        if ps_command.wait() : return False

        tmp = ps_output.split()
        
        assert self.pid == int(tmp[0])
        self.ppid = int(tmp[1])
        self.ruser = tmp[2]
        self.pcpu = float(tmp[3])
        self.rss = float(tmp[4])
        self.vsz = float(tmp[5])
        self.cmd = ' '.join(tmp[6:])

        return True

    def __repr__(self) :
        s = ""
        if self.pid: s += " pid: %d" % self.pid
        if self.ppid: s += " ppid: %d" % self.ppid
        if self.ruser: s += " user: " + self.ruser
        if self.pcpu: s += " cpu: %.2f" % self.pcpu
        if self.rss: s += " memory: %.2f" % self.rss
        if self.vsz: s += " virtual: %.2f" % self.vsz
        if self.cmd: s += " command: " + self.cmd 
        return s

class ResourceUsage :

    def __init__(self) :
        self.ppid = None
        self.pids = []
        self._set_variables_from_enviorment_variables()
        self.resource_usage_log = None
        self._run_command()
        return

    def _run_command(self) :
        process = popen(self.command_to_run, shell=True)
        self.ppid = process.pid
        assert self.ppid
        return

    def _get_all_processes(self) :
        my_user_id = environ['USER']
        self.pids = []
        if proc_status(self.ppid) :
            for pid in _get_all_processes_from(self.ppid) :
                ps = PSProcess(pid)
                if ps.ruser == my_user_id :
                    self.pids.append(ps)
        return

    def resource_usage(self) :
        if not self.resource_usage_log :
            assert self.resource_usage_log_file
            self.resource_usage_log = open(self.resource_usage_log_file, 'w')
            assert self.resource_usage_log
            self.resource_usage_log.write('        Time             CPU     Memory VirtualMem\n')
            self.resource_usage_log.flush()

        time_till_now = 0.0
        while 1 :
            self._get_all_processes()
            if len(self.pids) == 0 : break

            if time_till_now <= self.total_walltime_to_monitor_resource_usage :
                total_cpu_usage = 0.0
                total_rss_usage = 0.0
                total_vsz_usage = 0.0
                for pid in self.pids :
                    total_cpu_usage += pid.pcpu
                    total_rss_usage += pid.rss
                    total_vsz_usage += pid.vsz
                    
                total_rss_usage /= 1024*1024
                total_vsz_usage /= 1024*1024
            
                self.resource_usage_log.write('%20s %8.2f %8.2f %8.2f\n' % (current_time(), total_cpu_usage,
                                                                      total_rss_usage, total_vsz_usage))
                self.resource_usage_log.flush()
            else :
                if self.resource_usage_log :
                    self.resource_usage_log.close()
                
            sleep(self.period_to_monitor_resource_usage)
            time_till_now += self.period_to_monitor_resource_usage
            
        return

    def _set_variables_from_enviorment_variables(self) :
        self.total_walltime_to_monitor_resource_usage = 365*24*3600 
        self.period_to_monitor_resource_usage = 60 # 1 min
        self.command_to_run = None
        self.resource_usage_log_file = None

        if getenv('TOTAL_WALLTIME_TO_MONITOR_RESOURCE_USAGE') :
            self.total_walltime_to_monitor_resource_usage = \
                                atoi(getenv('TOTAL_WALLTIME_TO_MONITOR_RESOURCE_USAGE'))

        if getenv('PERIOD_TO_MONITOR_RESOURCE_USAGE') :
            self.period_to_monitor_resource_usage = atoi(getenv('PERIOD_TO_MONITOR_RESOURCE_USAGE'))

        if getenv('COMMAND_TO_RUN') :
            self.command_to_run = getenv('COMMAND_TO_RUN')
        else :
            die(' Please set enviorment variable COMMAND_TO_RUN')

        if getenv('RESOURCE_USAGE_LOG_FILE') :
            self.resource_usage_log_file = getenv('RESOURCE_USAGE_LOG_FILE')

        if not self.resource_usage_log_file :
            self.resource_usage_log_file = 'resource-'
            if getenv('PBS_JOBID') :
                self.resource_usage_log_file += 'pbsjob-' + getenv('PBS_JOBID').split('.')[0]
            else :
                self.resource_usage_log_file += 'pid-' + str(getpid())
            self.resource_usage_log_file += '-' + gethostname().split('.')[0] + '.log'

        assert self.resource_usage_log_file 

        return
    
if __name__ == "__main__" :

    if sys.version_info[0] != 2 or sys.version_info[1] < 5 :
        print("This script requires Python version newer than 2.5")
        exit(1)
        
    if len(argv) == 1:
        usage()

    for arg in argv[1:] :
        if arg == "-r" or arg == "-run" :
            ResourceUsage().resource_usage()
        elif arg == "-h" or arg == "-help" :
            usage()
        else :
            usage()

