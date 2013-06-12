#!/bin/env python

import os, sys, socket, time
from sys import argv, exit
from os import system, getenv, chmod, rename, getcwd
from string import atoi
import subprocess
from time import sleep as my_sleep

def die(information) :
    print information
    sys.exit(1)
    return

def assert_file_exist(file_name) :
    if(not os.path.exists(file_name)) :
        die("File '" + file_name + "' does not exit")
    return

def current_time() :
    return time.asctime(time.localtime())

def proc_status(pid):
    proc = "/proc/%d/status" % pid
    if os.path.exists(proc) :
        for line in open(proc).readlines() :
            if line.startswith("State:") :
                return line.split(":",1)[1].strip().split(' ')[0]
    return None

def _get_process_id_from_parent_pid(parent_pid, sleep_time = None) :
    if sleep_time : my_sleep(sleep_time+0.0)
    
    ps_command = subprocess.Popen("ps -o pid --ppid %d --noheaders 2>&1" % parent_pid,
                                  shell=True, stdout=subprocess.PIPE)
    ps_output = ps_command.stdout.read()
    if ps_command.wait() : return False
    
    process_id = None
    i = 0
    for pid_str in ps_output.split("\n")[:-1] :
        i += 1
        process_id = int(pid_str)
    assert i == 1
    return process_id

def get_process_id_from_parent_pid(parent_pid, sleep_time = None) :
    if not parent_pid : return None
    p_pid = parent_pid
    while 1 :
        pid = _get_process_id_from_parent_pid(p_pid, sleep_time)
        if not pid : break
        p_pid = pid
    return p_pid

def usage() :
    environment_variables = [
        "LOGIN_NODE_TO_SUBMIT_JOB",
        "PBS_SCRIPT_FILE",
        "COMMAND_TO_RUN",
        "BLCR_CHECK_POINT_FILE",
        "PERIOD_TO_CHECK_POINT",
        "TOTAL_WALLTIME",
        "USE_SAME_COMPUTE_NODE",
        "DONE_FILE"
        ]

    print
    print " Usage: " + argv[0] + " -run : to run jobs"
    print "        " + argv[0] + " -help : print this help information"
    print

    for env in environment_variables :
        print " export " + env + "="
    print
    exit()
    return

class JobWithBLCRCheckPoint :

    def __init__(self) :
        self.login_node_to_submit_job = "login-0-0"
        self.pbs_script_file = None
        self.command_to_run = None
        self.blcr_check_point_file = None
        self.period_to_check_point = 3600 ## default 1 hour
        self.total_time = 48*3600 ## default 48 hours in s48 queue
        self.use_same_compute_node = None
        self.done_file = None
        self._set_variables_from_enviorment_variables()
        self.process_id = None
        self.to_submit_a_new_job = None
        self._check_variables()
        return

    def __repr__(self) :
        s = ""

        s += " Login node to submit job:"
        if self.login_node_to_submit_job :  s += " " + self.login_node_to_submit_job
        s += "\n"
        
        s += " PBS script file:"
        if self.pbs_script_file : s += " " + self.pbs_script_file
        s += "\n"
        
        s += " Command to run:"
        if self.command_to_run: s += " " + self.command_to_run
        s += "\n"

        s += " BLCR checkpoint file:"
        if self.blcr_check_point_file: s += " " + self.blcr_check_point_file
        s += "\n"

        s += " Total wall time:"
        if self.total_time : s += (" %d" % self.total_time)
        s += "\n" 

        s += " Period to check point:"
        if self.period_to_check_point : s += (" %d" % self.period_to_check_point)
        s += "\n"

        s += " Use same compute node:"
        if self.use_same_compute_node : s += " YES"
        s += "\n"

        s += " Done file:"
        if self.done_file : s += " " + self.done_file
        s += "\n"
        
        return s
    
    def _set_variables_from_enviorment_variables(self) :
        if getenv("LOGIN_NODE_TO_SUBMIT_JOB") :
            self.login_node_to_submit_job = getenv("LOGIN_NODE_TO_SUBMIT_JOB")

        if getenv("PBS_SCRIPT_FILE") :
            self.pbs_script_file = getenv("PBS_SCRIPT_FILE")
        else :
            die(" Please set enviorment variable PBS_SCRIPT_FILE")
            
        if getenv("COMMAND_TO_RUN") :
            self.command_to_run = getenv("COMMAND_TO_RUN")
        else :
            die(" Please set enviorment variable COMMAND_TO_RUN")

        if getenv("BLCR_CHECK_POINT_FILE") :
            self.blcr_check_point_file = getenv("BLCR_CHECK_POINT_FILE")
        else :
            if getenv("PBS_JOBNAME") :
                self.blcr_check_point_file = getenv("PBS_JOBNAME") + ".blcr"

        if getenv("PERIOD_TO_CHECK_POINT") :
            self.period_to_check_point = atoi(getenv("PERIOD_TO_CHECK_POINT"))

        if(getenv("TOTAL_WALLTIME")) :
            self.total_time = atoi(getenv("TOTAL_WALLTIME"))
        elif getenv("PBS_WALLTIME") :
            self.total_time = atoi(getenv("PBS_WALLTIME"))

        if getenv("USE_SAME_COMPUTE_NODE") :
            if getenv("USE_SAME_COMPUTE_NODE") == "YES" :
                self.use_same_compute_node = True

        if getenv("DONE_FILE") :
            self.done_file = getenv("DONE_FILE")
        else :
            die(" Please set enviorment variable DONE_FILE")
                
        return

    def _check_variables(self) :
        assert self.login_node_to_submit_job
        assert self.pbs_script_file
        assert_file_exist(self.pbs_script_file)
        assert self.blcr_check_point_file
        assert self.done_file
        return

    def _set_process_id(self, parent_pid, sleep_time = None) :
        self.process_id = get_process_id_from_parent_pid(parent_pid, sleep_time)
        assert self.process_id
        return True
        
    def _print_process(self) :
        assert self.process_id
        command = "ps -o user,pid,%%cpu,%%mem,time,cmd --noheaders -p %d 2>&1" % self.process_id
        subprocess.check_call(command, shell=True)
        return
        
    def _run_job(self) :
        command = None
        if not os.path.exists(self.blcr_check_point_file) :
            command = "cr_run " + self.command_to_run
        else :
            print " Job will restart from BLCR file: ", self.blcr_check_point_file
            print
            command = "cr_restart --no-restore-pid " + self.blcr_check_point_file

        command += " && touch "  + self.done_file

        maximum_try = 5
        process = None
        i = 0
        while i < maximum_try :
            print " Try command: '" + command + "' %d" % i
            process = subprocess.Popen(command, shell=True)
            my_sleep(30.0)
            i += 1
            if self._set_process_id(process.pid) : break
        print
        
        if i == maximum_try :
            print " Restart failed"
            self.to_submit_a_new_job = True
            return

        print " Process id: %d" % self.process_id
        self._print_process()
        return

    def _do_check_point(self, command) :
        blcr_backup = self.blcr_check_point_file + ".backup"
        if os.path.exists(self.blcr_check_point_file) :
            rename(self.blcr_check_point_file, blcr_backup)
        
        pid_blcr = self.pid_blcr
        subprocess.check_call(command, shell=True)
        assert_file_exist(pid_blcr)
        chmod(pid_blcr, 0600)
        rename(pid_blcr, self.blcr_check_point_file)
        assert_file_exist(self.blcr_check_point_file)
        subprocess.check_call("ls -lt " + self.blcr_check_point_file, shell=True)
        print " Finished check point at:", current_time()
        self.to_submit_a_new_job = True
        return
 
    def do_check_point(self) :
        assert self.process_id

        self.pid_blcr = "context.%d" % self.process_id
        self.to_submit_a_new_job = True

        time_left = self.total_time
        command = None
        while time_left > self.period_to_check_point :
            my_sleep(self.period_to_check_point+0.0)
            time_left -= self.period_to_check_point

            if not proc_status(self.process_id) :
                if os.path.exists(self.done_file) :
                    print
                    print " Process %d does not exist, job has finished" % self.process_id
                    self.to_submit_a_new_job = False
                break

            print "\n Check point at: ", current_time()
            command = "cr_checkpoint --save-all --pid %d 2>&1" % self.process_id
            self._do_check_point(command)

        else :
            print "\n Check point and kill process %d at: %s" % (self.process_id, current_time())
            #command = "cr_checkpoint --save-all --term --pid %d 2>&1" % self.process_id
            command = "cr_checkpoint --save-all --pid %d 2>&1" % self.process_id
            self._do_check_point(command)
            my_sleep(60)

            # if cr_checkpoint fails to kill the process, we'll have to kill it from shell for 10 times
            command = "kill -9 %d" % self.process_id + " 2>&1"
            i = 0
            while i < 10 :
                if not proc_status(self.process_id) : break
                if i == 0 : print
                print " Try command: '" + command + "' %d" % i
                subprocess.check_call(command, shell=True)
                my_sleep(30)
                i += 1

        assert not proc_status(self.process_id)  
        return

    def _submit_new_job(self) :
        if not self.to_submit_a_new_job : return

        print
        print " Submit a new job from login node " + self.login_node_to_submit_job + " at " + current_time()

        qsub_args=""
        if self.use_same_compute_node :
            ppn = "1"
            if getenv("PBS_NUM_PPN") : ppn = getenv("PBS_NUM_PPN")
            qsub_args = "-l nodes=" + socket.gethostname().split(".")[0] + ":ppn=" + ppn + " "

        command = "ssh -x " + self.login_node_to_submit_job + " \"cd " + getcwd() + " &&" + \
                  " qsub " +  qsub_args + self.pbs_script_file + "\""

        subprocess.check_call(command, shell=True)
        return

    def run(self) :
        print 
        print " Job starts at:", current_time()
        print " Host:", socket.gethostname()
        print
        print self.__repr__()

        if os.path.exists(self.done_file) :
            return

        self._run_job()
        if not os.path.exists(self.done_file) and not self.to_submit_a_new_job :
            self.do_check_point()
        if not os.path.exists(self.done_file) :
            self._submit_new_job()

        print
        print " Job quits at:",  current_time()
        print
        return


if __name__ == "__main__" :

    if sys.version_info[0] != 2 or sys.version_info[1] < 5 :
        print("This script requires Python version newer than 2.5")
        exit(1)

    if len(argv) == 1:
        usage()

    for arg in argv[1:] :
        if arg == "-r" or arg == "-run" :
            JobWithBLCRCheckPoint().run()
        elif arg == "-h" or arg == "-help" :
            usage()
        else :
            usage()

    my_sleep(10)

    exit()


