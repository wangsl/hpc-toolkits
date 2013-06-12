#!/bin/env python

import os, sys, socket, time, re

def die(information) :
    print information
    sys.exit()
    return

def assert_file_exist(file_name) :
    if(not os.path.exists(file_name)) :
        die("File '" + file_name + "' does not exit")
    return

def is_blank_line(line) :
    blank_line_pattern = re.compile(r'^\s?$')
    if blank_line_pattern.match(line) :
        return True
    else :
        return None

def current_time() :
    return time.asctime(time.localtime())

class Thread :
    def __init__(self, pid, compute_node, job) :
        self.pid = pid
        self.compute_node = compute_node
        self.job = job
        return

    def __repr__(self) :
        s = ""
        if self.pid :
            s += " pid: %d\n" % (self.pid)
        s += " compute node: %s\n" % (self.compute_node)
        s += " job: %s" % (self.job)
        return s
        
    def run(self) :
        print " Job begin at:", current_time()
        print self
        command = "ssh -x " + self.compute_node + " \"" + self.job + "\""
        if os.getenv('MASTER_NODE_HOSTNAME') :
            if self.compute_node == os.getenv('MASTER_NODE_HOSTNAME') :
                command=self.job
        os.system(command)
        return

class SerialBatch :

    def __init__(self) :
        self.jobs = []
        self.compute_nodes = []
        self.threads = []
        self.free_compute_node = None
        return

    def __repr__(self) :
        s = ""
        s += "\n Compute node: %d\n" % (len(self.compute_nodes))
        for c in self.compute_nodes :
            s += " " + c + "\n"
            
        s += " Job number: %d\n" % (len(self.jobs))
        for j in self.jobs :
            s += " " + j + "\n"
            
        return s

    def read_jobs_from_file(self, file_name) :
        assert_file_exist(file_name)

        fin = open(file_name, "r")

        while 1 :
            line = fin.readline().rstrip("\n")
            if not line : break
            if is_blank_line(line) : continue
            self.jobs.append(line)
        fin.close()
        return
        
    def set_compute_nodes(self, nodes) :
        self.compute_nodes = nodes
        return

    def generate_compute_nodes(self) :
        self.compute_nodes = []
        if len(self.compute_nodes) == 0 :
            self._generate_compute_nodes_from_SGE()
        if len(self.compute_nodes) == 0 :
            self._generate_compute_nodes_from_PBS()
        return

    def _generate_compute_nodes_from_SGE(self) :
        self.compute_nodes = []
        if os.getenv('PE_HOSTFILE') :
            fin = open(os.getenv('PE_HOSTFILE'), "r")
            while 1 :
                line = fin.readline().rstrip("\n")
                if not line : break
                if is_blank_line(line) : continue
                tmp = line.split()
                node = tmp[0]
                n = int(tmp[1])
                for i in xrange(n) :
                    self.compute_nodes.append(node) 
        return

    def _generate_compute_nodes_from_PBS(self) :
        self.compute_nodes = []
        if os.getenv('PBS_NODEFILE') :
            fin = open(os.getenv('PBS_NODEFILE'), "r")
            while 1 :
                line = fin.readline().rstrip("\n")
                if not line : break
                if is_blank_line(line) : continue
                self.compute_nodes.append(line) 
        return

    def remove_current_hostname_from_compute_nodes(self) :
        current_hostname = socket.gethostname().split('.')[0]
        
        hostname_to_be_removed = None
        for c in self.compute_nodes :
            if c == current_hostname or c+".local" == current_hostname :
                hostname_to_be_removed = c
                break
        if hostname_to_be_removed : self.compute_nodes.remove(hostname_to_be_removed)
        return

    def check_free_compute_node(self) :
        while self.threads :
            pid, status = os.waitpid(0, os.WNOHANG)
            if not pid : break

            finished_thread = None
            for t in self.threads :
                if t.pid == pid :
                    finished_thread = t
                    break
            self.free_compute_node = None
            if finished_thread :
                self.free_compute_node = t.compute_node
                self.threads.remove(finished_thread)
        return

    def run(self) :
        self.generate_compute_nodes()
        assert len(self.compute_nodes) > 1
        self.remove_current_hostname_from_compute_nodes()

        i = 0
        while i < len(self.jobs) :
            self.check_free_compute_node()

            if not self.threads or len(self.threads) < len(self.compute_nodes) :
                if i < len(self.compute_nodes) :
                    self.free_compute_node = self.compute_nodes[i]
                    
                pid = os.fork()
                if not pid :
                    child_thread = Thread(pid = pid, compute_node = self.free_compute_node,
                                          job = self.jobs[i])
                    child_thread.run()
                    os._exit(0)
                else :
                    parent_thread = Thread(pid = pid, compute_node = self.free_compute_node,
                                           job = self.jobs[i])
                    self.threads.append(parent_thread)
                    i += 1
                
        while 1 :
            self.check_free_compute_node()
            if not self.threads : break
        
        return
        
## main program

if __name__ == "__main__" :

    assert len(sys.argv) == 2

    jobs_file = sys.argv[1]
    assert_file_exist(jobs_file)

    serial_batch = SerialBatch()
    serial_batch.read_jobs_from_file(jobs_file)

    #print serial_batch

    serial_batch.run()

    sys.exit()
