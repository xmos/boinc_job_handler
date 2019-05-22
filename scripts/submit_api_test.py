# This file is part of BOINC.
# http://boinc.berkeley.edu
# Copyright (C) 2016 University of California
#
# BOINC is free software; you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License
# as published by the Free Software Foundation,
# either version 3 of the License, or (at your option) any later version.
#
# BOINC is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with BOINC.  If not, see <http://www.gnu.org/licenses/>.

# test functions for submit_api.py

# YOU MUST CREATE A FILE "test_auth' CONTAINING
#
# project URL
# authenticator of your account

import os
import sys
import configparser
import time
import shutil
import random
import hashlib
import argparse
from submit_api import *

# read URL and auth from a file so we don't have to include it here
#
def get_auth():
    with open("test_auth", "r") as f:
        url = (f.readline()).strip()
        auth = (f.readline()).strip()
    return [url, auth]

# make a batch description, to be passed to estimate_batch() or submit_batch()
#
def make_batch_desc(test_cfg, app_cfg):
    file_info = """
        <file_info>
            <number>%s</number>
        </file_info>
        """
    file_info_exec = """
        <file_info>
            <number>%s</number>
            <executable/>
        </file_info>
        """
    file_ref = """
        <file_ref>
            <file_number>%s</file_number>
            <open_name>%s</open_name>
            <copy_file/>
        </file_ref>
        """
    all_exec_file_info = ""
    all_exec_file_ref = ""
    num_input_files = 1

    input_wav_files_descriptors = []
    for filename in test_cfg.input_files_on_server:
        file_desc = FILE_DESC()
        file_desc.mode = 'local_staged'
        file_desc.source = filename
        input_wav_files_descriptors.append(file_desc)

    executable_descs = []
    exec_count = num_input_files
    for platform, exe_info in test_cfg.executable_info_dict.items():
        fdesc = FILE_DESC()
        fdesc.mode = 'local_staged'
        fdesc.source = exe_info.boinc_name
        all_exec_file_info += file_info_exec%(str(exec_count))
        all_exec_file_ref += file_ref%(str(exec_count), exe_info.logical_name)
        executable_descs.append(fdesc)
        exec_count += 1

    batch = BATCH_DESC()
    [batch.project, batch.authenticator] = get_auth()
    batch.app_name = test_cfg.app_name
    batch.batch_name = test_cfg.batch_name
    #batch.app_version_num = int(app_version)
    batch.batch_id = test_cfg.batch_id
    batch.jobs = []


    for i in range(len(input_wav_files_descriptors)):
        job = JOB_DESC()
        job.delay_bound = 60
        job.files = [input_wav_files_descriptors[i]] 
        for exe in executable_descs:
            job.files.append(exe)
        #create file_ref and file_info for input file
        all_input_file_info = "" 
        all_input_file_info += (file_info%(str(0)))

        all_input_file_ref = ""
        all_input_file_ref += (file_ref%(str(0),app_cfg.input_file_logical_name))

        if True:
            job.input_template = """
<input_template>
    %s
    %s
    <workunit>
        %s
        %s
        <target_nresults>1</target_nresults>
        <min_quorum>1</min_quorum>
        <credit>%d</credit>
        <rsc_fpops_est>%f</rsc_fpops_est>
        <rsc_memory_bound>130e6</rsc_memory_bound>
        <rsc_disk_bound>130e6</rsc_disk_bound>
    </workunit>
</input_template>
""" % (all_input_file_info, all_exec_file_info, all_input_file_ref, all_exec_file_ref, i+1, (i+1)*1e10)
        job.output_template = create_output_template(test_cfg, app_cfg)
        batch.jobs.append(copy.copy(job))

    return batch

def test_estimate_batch(batch_name, batch_id):
    batch = make_batch_desc(batch_name, batch_id)
    #print batch.to_xml("submit")
    r = estimate_batch(batch)
    if check_error(r):
        return
    print 'estimated time: ', r[0].text, ' seconds'

def test_submit_batch(test_cfg, app_cfg):
    batch = make_batch_desc(test_cfg, app_cfg)
    r = submit_batch(batch)
    print r
    if check_error(r):
        assert False, "submit_batch returned error"
        return
    print 'batch ID: ', r[0].text

def test_query_batches():
    req = REQUEST()
    [req.project, req.authenticator] = get_auth()
    req.get_cpu_time = True
    r = query_batches(req)
    if check_error(r):
        return
    print ET.tostring(r)

def test_query_batch(id):
    req = REQUEST()
    [req.project, req.authenticator] = get_auth()
    req.batch_id = id
    req.get_cpu_time = True
    req.get_job_details = True
    r = query_batch(req)
    if check_error(r):
        assert False, "query_batch returned error"
    '''
    print ET.tostring(r)
    print 'njobs: ', r.find('njobs').text
    print 'fraction done: ', r.find('fraction_done').text
    print 'total CPU time: ', r.find('total_cpu_time').text
    # ... various other fields
    print 'jobs:'
    for job in r.findall('job'):
        print '   id: ', job.find('id').text
        # ... various other fields
    '''
    return r

def test_create_batch(name, app_name):
    req = CREATE_BATCH_REQ()
    [req.project, req.authenticator] = get_auth()
    req.app_name = app_name
    req.batch_name = name
    req.expire_time = 0
    
    r = create_batch(req)

    if check_error(r):
        assert(False),"create_batch returned error"
        return
    print 'batch ID: ', r[0].text
    return r[0].text

def test_abort_batch(batch_id):
    req = REQUEST()
    [req.project, req.authenticator] = get_auth()
    req.batch_id = batch_id
    r = abort_batch(req)
    if check_error(r):
        return
    print 'success'

def test_retire_batch(batch_id):
    req = REQUEST()
    [req.project, req.authenticator] = get_auth()
    req.batch_id = batch_id
    r = retire_batch(req)
    if check_error(r):
        assert(False),"retire jobs failed"
    return

def test_upload_files(local_names, boinc_names, batch_id):
    req = UPLOAD_FILES_REQ()
    [req.project, req.authenticator] = get_auth()
    req.batch_id = batch_id
    req.local_names = local_names
    req.boinc_names = boinc_names
    print req.boinc_names
    r = upload_files(req)
    if check_error(r):
        assert(False),"upload_files returned error"
        return
    print 'upload_files: success'

def test_query_files(boinc_names_list, batch_id):
    req = QUERY_FILES_REQ()
    [req.project, req.authenticator] = get_auth()
    req.batch_id = batch_id
    req.boinc_names = boinc_names_list
    r = query_files(req)
    if check_error(r):
        assert(False),"query_files returned error"
        return
    print 'absent files:'
    for f in r[0]:
        print f.text

def test_get_output_file(job_name, file_num):
    req = REQUEST()
    [req.project, req.authenticator] = get_auth()
    req.instance_name = job_name
    req.file_num = file_num
    r = get_output_file(req)
    print(r)
    return r

def test_get_output_files(batch_id):
    req = REQUEST()
    [req.project, req.authenticator] = get_auth()
    req.batch_id = batch_id
    r = get_output_files(req)
    print(r)

def test_get_job_counts():
    req = REQUEST()
    req.project = 'http://boinc-server.xmos.com/hellopython/'
    x = get_job_counts(req)
    print x.find('results_ready_to_send').text


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("app_cfg_file", help="app config file",)
    parser.add_argument("batch_cfg_file", help="batch config file")
    parser.add_argument("--platforms_supported", nargs="*",type=str,default=[])
    parser.parse_args()
    args = parser.parse_args()
    return args


def create_output_template(test_cfg, app_cfg):
    file_info = """
        <file_info>
            <name><OUTFILE_%s/></name>
            <generated_locally/>
            <upload_when_present/>
            <max_nbytes>125e6</max_nbytes>
            <url><UPLOAD_URL/></url>
        </file_info>
        """

    file_ref = """
            <file_ref>
                <file_name><OUTFILE_%s/></file_name>
                <open_name>%s</open_name>
                <copy_file/>
            </file_ref>
        """
    all_file_info = ""
    all_file_ref = ""
    for i in range(len(app_cfg.output_files_logical_names)):
        all_file_info += (file_info%(str(i)))
        all_file_ref += (file_ref%(str(i), app_cfg.output_files_logical_names[i]))

    full_output_template = """
    <output_template>
        %s
        <result>
            %s
        </result>
    </output_template>
    """%(all_file_info, all_file_ref)
    return full_output_template

class executable_info(object):
    def __init__(self, platform, local_name, boinc_name, logical_name):
        self.platform = platform
        self.local_name = local_name
        self.boinc_name = boinc_name
        self.logical_name = logical_name

class batch_config_params(object):
    def __init__(self, app_name, input_dir, output_dir, output_files, executable_local_names, app_cfg):
        self.app_cfg = app_cfg
        self.app_name = app_name 
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.output_files_list = output_files
        
        #get input file list
        self.input_files_on_server = []
        self.local_files_list = get_files(self.input_dir) #get a list of wav files present in the input files directory
        assert(len(self.local_files_list) > 0), "no wav files in %s folder"%(self.input_dir)

        for f in self.local_files_list:
            print f, os.path.basename(f)
            filename_on_server = os.path.basename(f);
            self.input_files_on_server.append(filename_on_server)

        #make sure the no. of output file the app generates is the same as no. of output files the user expects
        assert(len(self.output_files_list) == len(app_cfg.output_files_logical_names)),"user expects %d \
                            output files while app generates %d"%(len(self.output_files_list), len(app_cfg.output_files_logical_names))

        #get executable info
        self.executable_info_dict = {}
        #look for platforms that the app supports
        for platform, exec_logical_name in app_cfg.executable_logical_names_dict.items(): #for every platform that the app supports
            if platform in executable_local_names: #if the user has provided an executable for that platform
                exec_local_name = executable_local_names[platform]
                exec_boinc_name = os.path.basename(exec_local_name) + '_' + str(get_md5_hash_filename(exec_local_name)) 
                exec_info = executable_info(platform, exec_local_name, exec_boinc_name, exec_logical_name)
                self.executable_info_dict[platform] = exec_info

        assert(len(self.executable_info_dict) > 0),"no exceutable found in config file"


class app_config_params(object):
    def __init__(self, app_name, input_file, output_files_list, executable_logical_names):
        self.app_name = app_name
        self.input_file_logical_name = input_file
        self.output_files_logical_names = output_files_list
        self.executable_logical_names_dict = executable_logical_names


def parse_test_cfg_file(cfg_file, app_cfg, platforms_supported):
    config = configparser.ConfigParser()
    config.read(cfg_file)
    #parse test_params
    app_name = config.get('test_params', 'app_name')
    input_path = config.get('test_params', 'input_files_directory')
    output_path = config.get('test_params', 'output_files_directory')
    output_files = config.get('test_params', 'output_filenames')
    
    executable_local_names = {} #dictionary containing executable local names

    for platform in platforms_supported:
        executable = None
        try:
            executable = config.get('executables_local_names', platform)
        except:
            pass
        if executable is not None:
            executable_local_names[platform] = executable

    output_files_list = output_files.split()
    return batch_config_params(app_name, input_path, output_path, output_files_list, executable_local_names, app_cfg)


def parse_app_cfg_file(cfg_file, platforms_supported):
    config = configparser.ConfigParser()
    config.read(cfg_file)
    sections = config.sections()
    app_name = config.get('app_params', 'app_name')
    input_file = config.get('app_params', 'input_file_logical_name')
    output_files = config.get('app_params', 'output_files_logical_name')

    executable_logical_names = {} #dictionary containing executable local names

    for platform in platforms_supported:
        executable = None
        try:
            executable = config.get('executable_logical_names', platform)
        except:
            pass
        if executable is not None:
            executable_logical_names[platform] = executable

    output_files_list = output_files.split()
    return app_config_params(app_name, input_file, output_files_list, executable_logical_names)
    test_params = parse_test_params(config, 'test_params')
    return test_params

'''
Go through a directory tree and get all wav files
'''
def get_files(input_dir):
    '''
    walk through a given directory tree structure and list out all the wav files
    '''
    file_list = []
    for root, dirs, files in os.walk(os.path.abspath(input_dir)):
        for f in files:
            if f.endswith(".wav"):
                file_list.append(os.path.join(root,f))
    return file_list

'''
given a list of jobs, download output files corresponding to each job
'''
def download_output_files(test_cfg, jobnames):
    num_output_files_per_job = 1
    assert(len(jobnames) == len(test_cfg.input_files_on_server)), "num input files (%d) != num jobs (%d)"%(len(test_cfg.input_files_on_server), len(jobnames)) 

    for i in range(len(jobnames)):
        #create one output directory per job
        output_path = os.path.join(test_cfg.output_dir, os.path.splitext(test_cfg.input_files_on_server[i])[0])
        if os.path.exists(output_path):
            shutil.rmtree(output_path)
        os.makedirs(output_path)

        for j in range(len(test_cfg.output_files_list)):
            output_name = os.path.join(output_path, test_cfg.output_files_list[j])
            download_url = test_get_output_file(jobnames[i], str(j))
            r = requests.get(download_url)
            assert(r.status_code == requests.codes.ok),"requests.get(download_url) returns error"
                
            with open(output_name, 'wb') as fd:
                for chunk in r.iter_content():
                    fd.write(chunk)
                fd.close()

def get_md5_hash_filename(filename):
    with open(filename,"rb") as f:
        bytes = f.read() # read file as bytes
        readable_hash = hashlib.md5(bytes).hexdigest();
        print(readable_hash)
    return readable_hash
    

def upload_input_files(test_cfg):
    #upload executables
    local_names = []
    boinc_names = []
    for info in test_cfg.executable_info_dict.values():
        local_names.append(info.local_name)
        boinc_names.append(info.boinc_name)

    test_upload_files(local_names, boinc_names, test_cfg.batch_id)
    
    #upload input files
    test_upload_files(test_cfg.local_files_list, test_cfg.input_files_on_server, test_cfg.batch_id)


def test(app_cfg_file, batch_cfg_file, platforms_supported):
    assert(len(platforms_supported) > 0),"no platforms specified. Provide --platform_supported argument on the command line"
    
    app_cfg = parse_app_cfg_file(app_cfg_file, platforms_supported)
    test_cfg = parse_test_cfg_file(batch_cfg_file, app_cfg, platforms_supported)

    assert(test_cfg.app_name == app_cfg.app_name),"app_name in test and app config files doesn't match"
    if os.path.exists(test_cfg.output_dir):
        shutil.rmtree(test_cfg.output_dir)
    os.makedirs(test_cfg.output_dir)

    app_name = test_cfg.app_name
    batch_name = app_name + '_' + str(random.randint(1,1234565))
    test_cfg.batch_name = batch_name

    test_cfg.batch_id = test_create_batch(batch_name, app_name) #batch_id is a string
    print('created batch ',test_cfg.batch_id)

    upload_input_files(test_cfg)
    test_submit_batch(test_cfg, app_cfg)
    
    query_return = test_query_batch(test_cfg.batch_id) 
    status = query_return.find('state').text
    jobnames = []
    for job in query_return.findall('job'):
        jobnames.append(job.find('name').text + '_' + '0')
    
    #wait for batch to complete
    while status == '0' or status == '1':
        time.sleep(5)
        print 'batch_id %s query status'%(test_cfg.batch_id)
        query_return = test_query_batch(test_cfg.batch_id) 
        status = query_return.find('state').text
        print 'batch_id %s status is %s'%(test_cfg.batch_id, status)

    assert(status == '2'), "batch completed with invalid status %s"%(status)
    
    #download output files
    download_output_files(test_cfg, jobnames)

    test_retire_batch(test_cfg.batch_id)

if __name__ == "__main__":
    args = parse_arguments()
    test(args.app_cfg_file, args.batch_cfg_file, args.platforms_supported)
