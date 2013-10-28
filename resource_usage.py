#!/bin/env python

# $Id$

# ps -o pid,ppid,ruser,pcpu,rss,vsz,cmd --user sw77
# ps -o pid,ppid,ruser,pcpu,rss,vsz,cmd --ppid 4530

import subprocess

def list_uniq(alist) :
    """
    Fastest order preserving
    """
    set = {}
    return [set.setdefault(e,e) for e in alist if e not in set]


def _subprocess_from(ppid) :
    assert ppid
    ps_command = subprocess.Popen("ps --no-headers -o pid --ppid %d 2>&1" % ppid,
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
        ps_command = subprocess.Popen("ps --no-headers -o pid,ppid,ruser,pcpu,rss,vsz,cmd --pid %d 2>&1" % self.pid,
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

    def __init__(self, command) :
        self.command = command
        return

if __name__ == "__main__" :

    print "test"

    command = "ps --no-headers -o pid,ppid,ruser,pcpu,rss,vsz,cmd --ppid 4530 2>&1 | awk '{print $1}' | xargs"
    
    ps_command = subprocess.Popen(command, 
                                  shell=True, stdout=subprocess.PIPE)
    ps_output = ps_command.stdout.read()
    assert not ps_command.wait()
    
    tmp = ps_output.split()

    for pid in tmp :
        ps_test = PSProcess(pid)
        print ps_test

    for pid in _get_all_processes_from(13884) :
        ps_test = PSProcess(pid)
        print ps_test

    
    
