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

# YOU MUST CREATE A FILE authorization file CONTAINING
#
# project URL
# authenticator of your account
# The name of the authorization file is passed in the command line arguments.

import os
import sys
import configparser
import time
import shutil
import random
import hashlib
import argparse
import json
import numpy as np
from batch_process_api import *

batch_state = {"BATCH_STATE_INIT":"0", "BATCH_STATE_IN_PROGRESS":"1", "BATCH_STATE_COMPLETE":"2", "BATCH_STATE_ABORTED":"3", "BATCH_STATE_RETIRED":"4"}
# read URL and auth from a file so we don't have to include it here
#
def get_auth():
    with open(auth_file, "r") as f:
        url = (f.readline()).strip()
        auth = (f.readline()).strip()
    return [url, auth]

# make a batch description, to be passed to estimate_batch() or submit_batch()
#
def make_batch_desc(batch_cfg):
    file_info = """
        <file_info>
            <number>%s</number>
        </file_info>
        """
    file_info_exec = """
        <file_info>
            <number>%s</number>
            %s
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
    
    #create file descriptors for input files.
    #These refer to files in batch_config["input_files_directory"]. There will be one job for every file in batch_config["input_files_directory"]
    input_wav_files_descriptors = []
    for filename in batch_cfg.input_files_on_server:
        file_desc = FILE_DESC()
        file_desc.mode = 'local_staged'
        file_desc.source = filename
        input_wav_files_descriptors.append(file_desc)
    
    #Create file descriptors for all other input files. These refer to the files specified in batch_config["job_application_files"]
    #and batch_config["other_input_files"] section.
    #Unlike, files in "input_files_directory", these files don't dictate the no. of jobs. Every job has these files as its input files
    other_input_file_descs = []
    exec_count = 1 #count starting from 1 since the 0th file descriptor refers to the input file that decides the job
    for f in batch_cfg.all_application_files:
        fdesc = FILE_DESC()
        fdesc.mode = 'local_staged'
        fdesc.source = f["boinc_name"]
        exec_str = "<executable/>"
        if(f["executable"] == True):
            all_exec_file_info += file_info_exec%(str(exec_count), exec_str)
        else:
            all_exec_file_info += file_info_exec%(str(exec_count), " ")

        all_exec_file_ref += file_ref%(str(exec_count), f["logical_name"])
        other_input_file_descs.append(fdesc)
        exec_count += 1

    batch = BATCH_DESC()
    [batch.project, batch.authenticator] = get_auth()
    batch.app_name = batch_cfg.app_name
    batch.batch_name = batch_cfg.batch_name
    #batch.app_version_num = int(app_version)
    batch.batch_id = batch_cfg.batch_id
    batch.jobs = []


    for i in range(len(input_wav_files_descriptors)):
        job = JOB_DESC()
        job.delay_bound = 60
        job.files = [input_wav_files_descriptors[i]] 
        for desc in other_input_file_descs:
            job.files.append(desc)
        #create file_ref and file_info the input file which decides the job
        all_input_file_info = "" 
        all_input_file_info += (file_info%(str(0)))

        all_input_file_ref = ""
        all_input_file_ref += (file_ref%(str(0), batch_cfg.input_file_logical_name))

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
        job.output_template = create_output_template(batch_cfg)
        batch.jobs.append(copy.copy(job))
    
        print job.input_template
    return batch


def submit_batch(batch_cfg):
    batch = make_batch_desc(batch_cfg)
    r = submit_batch_core(batch)
    print r
    if check_error(r):
        assert False, "submit_batch returned error"
        return
    print 'batch ID: ', r[0].text


def query_batch(id):
    req = REQUEST()
    [req.project, req.authenticator] = get_auth()
    req.batch_id = id
    req.get_cpu_time = True
    req.get_job_details = True
    r = query_batch_core(req)
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

def create_batch(name, app_name):
    req = CREATE_BATCH_REQ()
    [req.project, req.authenticator] = get_auth()
    req.app_name = app_name
    req.batch_name = name
    req.expire_time = 0
    
    r = create_batch_core(req)

    if check_error(r):
        assert(False),"create_batch returned error"
        return
    #print 'batch ID: ', r[0].text
    return r[0].text

def abort_batch(batch_id):
    req = REQUEST()
    [req.project, req.authenticator] = get_auth()
    req.batch_id = batch_id
    r = abort_batch_core(req)
    if check_error(r):
        return
    print 'success'

def retire_batch(batch_id):
    req = REQUEST()
    [req.project, req.authenticator] = get_auth()
    req.batch_id = batch_id
    r = retire_batch_core(req)
    if check_error(r):
        assert(False),"retire jobs failed"
    return

def upload_files(local_names, boinc_names, batch_id):
    req = UPLOAD_FILES_REQ()
    [req.project, req.authenticator] = get_auth()
    req.batch_id = batch_id
    req.local_names = local_names
    req.boinc_names = boinc_names
    print req.boinc_names
    r = upload_files_core(req)
    if check_error(r):
        assert(False),"upload_files returned error"
        return
    print 'upload_files: success'


def get_output_file(job_name, file_num):
    req = REQUEST()
    [req.project, req.authenticator] = get_auth()
    req.instance_name = job_name
    req.file_num = file_num
    r = get_output_file_core(req)
    print(r)
    return r



def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("app_cfg_file", help="app config file",)
    parser.add_argument("batch_cfg_file", help="batch config file")
    parser.add_argument("auth_file", help="authorization details of the user trying to submit a batch")
    parser.add_argument("--platforms_supported", nargs="*",type=str,default=[])
    parser.parse_args()
    args = parser.parse_args()
    return args


def create_output_template(batch_cfg):
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
    for i in range(len(batch_cfg.output_files_list)):
        all_file_info += (file_info%(str(i)))
        all_file_ref += (file_ref%(str(i), batch_cfg.output_files_list[i]))

    full_output_template = """
    <output_template>
        %s
        <result>
            %s
        </result>
    </output_template>
    """%(all_file_info, all_file_ref)
    return full_output_template

def get_application_files_from_batch_config(batch_config, category, platform):
    platform_files = [pf for pf in batch_config[category] if pf["platform"] == platform]
    assert(len(platform_files) <= 1)
    if(len(platform_files) > 0):
        return platform_files[0]["files"]
    else:
        return None

def get_application_files_logical_names_from_app_config(app_config, platform):
    platform_files = [pf for pf in app_config["job_application_files"] if pf["platform"] == platform]
    assert(len(platform_files) <= 1)
    if(len(platform_files) > 0):
        return platform_files[0]["files"]
    else:
        return None


class batch_config_params(object):
    def __init__(self, platforms_supported, batch_config):
        self.app_name = batch_config["app_name"] 
        self.input_dir = batch_config["input_files_directory"]
        self.input_files_ext = batch_config["input_files_extension"] 
        self.input_file_logical_name = batch_config["input_file_logical_name"]
        self.output_dir = batch_config["output_files_directory"]
        self.output_files_list = batch_config["output_filenames"]
        '''
        Notes on different kinds of input files:
            There are 3 kinds of input files:
            1. The input files present in batch_config["input_files_directory"]. These are files that determine the number of jobs.
               There is one job per input file present in the "input_files_directory". These files are essentially what you're testing the application for.

            2. The second kind are what are provided in batch_config["job_application_files"]. These are the application files that the job executes 
               sequentially. If you look in the job.xml for the application on the server, it would mention these applications with their logical names
               one after another. Since these applications are referred to in job.xml, their logical names present in app config should match the logical
               names present in batch config.

            3. The third kind are clubbed into a generic group batch_config["other_input_files"]. These could be input files common to all jobs. These 
               files could be executables or simply data files. They may or may not be platform dependent. If these are platform dependent, their logical 
               name would be different for each platform. The caller then needs to know the platform dependent name and has to be compiled accordingly.
               If the file happens to be platform independent, it can have the same logical name on every platform. It still needs to be mentioned as an 
               input file for every platform"
        '''

        #get input file list
        self.input_files_on_server = []
        self.local_files_list = get_files(self.input_dir, self.input_files_ext) #get a list of files present in the input files directory
        assert(len(self.local_files_list) > 0), "no wav files in %s folder"%(self.input_dir)
        
        #the input files are stored with just their filenames on the server
        for f in self.local_files_list:
            filename_on_server = os.path.basename(f);
            self.input_files_on_server.append(filename_on_server)

        #get a list of application files
        self.all_application_files = []
        for plat in platforms_supported:
            app_names_from_batch_cfg = get_application_files_from_batch_config(batch_config, "job_application_files", plat)
            if app_names_from_batch_cfg != None:
                self.all_application_files.append(app_names_from_batch_cfg) 

            app_names_from_batch_cfg = get_application_files_from_batch_config(batch_config, "other_platform_dependent_input_files", plat)
            if app_names_from_batch_cfg != None:
                self.all_application_files.append(app_names_from_batch_cfg) 

        #get platform dependent other input files
        common_files = [f for f in batch_config["other_common_input_files"]]
        if(len(common_files) > 0):
            self.all_application_files.append(common_files)


        self.all_application_files = list(np.array(self.all_application_files).flat)

        #add "boinc_name" field to the application_files info
        for f in self.all_application_files:
            boinc_name = os.path.basename(f["local_name"]) + '_' + str(get_md5_hash_filename(f["local_name"])) 
            f["boinc_name"] = boinc_name


def parse_cfg_files(app_cfg_file, batch_cfg_file, platforms_supported):
    fbatch = open(batch_cfg_file, "r")
    batch_config = json.load(fbatch)
    fbatch.close()

    fapp = open(app_cfg_file, "r")
    app_config = json.load(fapp)
    fapp.close()

    assert(batch_config["app_name"] == app_config["app_name"]),"app_name in test and app config files doesn't match"
    
    runnable_application = False
    #check if we have any runnable applications
    for plat in platforms_supported:
        app_names_from_batch_cfg = get_application_files_from_batch_config(batch_config, "job_application_files", plat)
        app_names_from_app_cfg = get_application_files_logical_names_from_app_config(app_config, plat)
        if(app_names_from_batch_cfg == None and app_names_from_app_cfg != None ):
            print ("WARNING: application supports platform {} but batch doesn't provide application files for this platform".format(plat))
        elif(app_names_from_batch_cfg != None and app_names_from_app_cfg == None ):
            print ("WARNING: application doesn't support platform {} but batch provides application files for this platform".format(plat))
        elif(app_names_from_batch_cfg != None and app_names_from_app_cfg != None ):
            assert(len(app_names_from_batch_cfg) == len(app_names_from_app_cfg)), "no. of application files provided by batch different from what app expects"
            for i in range(len(app_names_from_app_cfg)):
                assert(app_names_from_app_cfg[i]["logical_name"] == app_names_from_batch_cfg[i]["logical_name"]),"logical name of application file in batch and app cfg differs" 
            runnable_application = True #if we made it here, means we have atleast one set of application files in the batch that can be run
    assert(runnable_application == True), "no runnable application found. check warnings above"

    return batch_config_params(platforms_supported, batch_config)


'''
Go through a directory tree and get all wav files
'''
def get_files(input_dir, extension):
    '''
    walk through a given directory tree structure and list out all the wav files
    '''
    file_list = []
    for root, dirs, files in os.walk(os.path.abspath(input_dir)):
        for f in files:
            if extension != None:
                if f.endswith(extension):
                    file_list.append(os.path.join(root,f))
            else:
                file_list.append(os.path.join(root,f))
    return file_list

'''
given a list of jobs, download output files corresponding to each job
'''
def download_output_files(batch_cfg, jobnames):
    num_output_files_per_job = 1
    assert(len(jobnames) == len(batch_cfg.input_files_on_server)), "num input files (%d) != num jobs (%d)"%(len(batch_cfg.input_files_on_server), len(jobnames)) 

    for i in range(len(jobnames)):
        #create one output directory per job
        output_path = os.path.join(batch_cfg.output_dir, os.path.splitext(batch_cfg.input_files_on_server[i])[0])
        if os.path.exists(output_path):
            shutil.rmtree(output_path)
        os.makedirs(output_path)

        for j in range(len(batch_cfg.output_files_list)):
            output_name = os.path.join(output_path, batch_cfg.output_files_list[j])
            download_url = get_output_file(jobnames[i], str(j))
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
    return readable_hash
    

def upload_input_files(batch_cfg):
    #upload executables
    local_names = []
    boinc_names = []
    for f in batch_cfg.all_application_files:
        local_names.append(f["local_name"])
        boinc_names.append(f["boinc_name"])

    upload_files(local_names, boinc_names, batch_cfg.batch_id)
    
    #upload input files
    upload_files(batch_cfg.local_files_list, batch_cfg.input_files_on_server, batch_cfg.batch_id)


def process_batch(app_cfg_file, batch_cfg_file, platforms_supported):
    assert(len(platforms_supported) > 0),"no platforms specified. Provide --platform_supported argument on the command line"
    
    batch_cfg = parse_cfg_files(app_cfg_file, batch_cfg_file, platforms_supported)

    return

    if os.path.exists(batch_cfg.output_dir):
        shutil.rmtree(batch_cfg.output_dir)
    os.makedirs(batch_cfg.output_dir)

    app_name = batch_cfg.app_name
    batch_name = app_name + '_' + str(random.randint(1,1234565))
    batch_cfg.batch_name = batch_name

    batch_cfg.batch_id = create_batch(batch_name, app_name) #batch_id is a string
    print('created batch ',batch_cfg.batch_id)

    upload_input_files(batch_cfg)
    submit_batch(batch_cfg)
    
    query_return = query_batch(batch_cfg.batch_id) 
    status = query_return.find('state').text
    jobnames = []
    for job in query_return.findall('job'):
        jobnames.append(job.find('name').text + '_' + '0')
    
    #wait for batch to complete
    while status == batch_state["BATCH_STATE_INIT"]  or status == batch_state["BATCH_STATE_IN_PROGRESS"]:
        time.sleep(5)
        print 'batch_id %s query status'%(batch_cfg.batch_id)
        query_return = query_batch(batch_cfg.batch_id) 
        status = query_return.find('state').text
        print 'batch_id %s status is %s'%(batch_cfg.batch_id, status)

    assert(status == batch_state["BATCH_STATE_COMPLETE"]), "batch completed with invalid status %s"%(status)
    
    #download output files
    download_output_files(batch_cfg, jobnames)

    retire_batch(batch_cfg.batch_id)

auth_file=""
if __name__ == "__main__":
    args = parse_arguments()
    auth_file = args.auth_file
    process_batch(args.app_cfg_file, args.batch_cfg_file, args.platforms_supported)
