import os
import subprocess
import shutil
import argparse
import json

root_dir="../"

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("app_cfg_file", help="app config file",)
    parser.add_argument("--platforms_supported", nargs="*",type=str,default=[])
    parser.parse_args()
    args = parser.parse_args()
    return args

class app_config_params(object):
    def __init__(self, app_name, app_desc, input_file, output_files_list, executable_logical_names, wrapper_names):
        self.app_name = app_name
        self.app_desc = app_desc
        self.input_file_logical_name = input_file
        self.output_files_logical_names = output_files_list
        self.executable_logical_names_dict = executable_logical_names
        self.wrapper_names_dict = wrapper_names

def parse_app_cfg_file(app_cfg_file, platforms_supported):
    fapp = open(app_cfg_file, "r")
    app_config = json.load(fapp)
    fapp.close()
    return app_config

def create_app(app_cfg_file, platforms_supported):
    job_info = """
<job_desc>
    %s
</job_desc>
    """

    job_task_info = """
        <task>
            <application>%s</application>
        </task>
    """

    version_info = """
<version>
   <file>
      <physical_name>%s</physical_name>
      <main_program/>
   </file>
   <file>
      <physical_name>%s</physical_name>
      <logical_name>job.xml</logical_name>
   </file>
   %s
</version>
    """

    file_info = """
       <file>
          <physical_name>%s</physical_name>
          <logical_name>%s</logical_name>
          <copy_file/>
       </file>
    """

    app_info = """
	<app>
	    <name>%s</name>
	    <user_friendly_name>%s</user_friendly_name>
	</app>
    """
    app_daemon = """
        <daemon>
          <cmd>
            %s -d 4 -app %s
          </cmd>
        </daemon>
    """
    print("Creating application directory in the apps folder")
    assert(len(platforms_supported) > 0),"no platforms specified. Provide --platform_supported argument on the command line"
    app_cfg = parse_app_cfg_file(app_cfg_file, platforms_supported)
    path_to_app = os.path.join(root_dir+"apps", app_cfg["app_name"])
    new_ver = "1.0"
    if os.path.exists(path_to_app):
        imm_subdirs = next(os.walk(path_to_app))[1] #get immediate subdirectories to the apps directory
        existing_vers = [float(y) for y in imm_subdirs]
        new_ver = str(max(existing_vers) + 1.0)
    
    print new_ver	
    version_dir = os.path.join(path_to_app, new_ver)
    os.makedirs(version_dir)

    for plat in platforms_supported:
        plat_dir = os.path.join(version_dir, plat)
        os.makedirs(plat_dir)

        #find wrapper file for platform
        plat_plus_wrapper_file = [pf for pf in app_cfg["python_wrapper_file"] if pf["platform"] == plat]
        if(len(plat_plus_wrapper_file) > 0):
            #copy the wrapper file
            shutil.copy2(plat_plus_wrapper_file[0]["filename"], plat_dir)
        
        #copy job application files if present. If the physical_name is specified, means that these files are to be copied 
        #on the server as part of app_create. If physical_name is not present, it means these files will be given as part of
        #batch submit.
        plat_plus_job_app_files = [pf for pf in app_cfg["job_application_files"] if pf["platform"] == plat]
        assert(len(plat_plus_job_app_files) <= 1) #each platform mentioned only once
        if(len(plat_plus_job_app_files) > 0):
            for f in plat_plus_job_app_files[0]["files"]:
                if(f["physical_name"] != None):
                    shutil.copy2(f["physical_name"], plat_dir)

        #copy any other input files 
        plat_plus_other_input_files = [pf for pf in app_cfg["other_platform_dependent_input_files"] if pf["platform"] == plat]
        assert(len(plat_plus_other_input_files) <= 1) #each platform mentioned only once
        if(len(plat_plus_other_input_files) > 0):
            for f in plat_plus_other_input_files[0]["files"]:
                assert(f["physical_name"] != None), "physical file not specified for a file provided as input to app"
                shutil.copy2(f["physical_name"], plat_dir)

        other_common_files = [f for f in app_cfg["other_common_input_files"]]
        for f in other_common_files:
            assert(f["physical_name"] != None), "physical file not specified to a file that needs to be copied on server"
            shutil.copy2(f["physical_name"], plat_dir)
        
        #write job.xml file
        job_task_str = ""
        if(len(plat_plus_job_app_files) > 0):
            for pf in plat_plus_job_app_files[0]["files"]:
                job_task_str += (job_task_info%(pf["logical_name"]))
        job_info_str = job_info % job_task_str

        job_file_name = app_cfg["app_name"] + "_job" + "_" + new_ver + "_" + plat
        job_file_full = os.path.join(plat_dir, job_file_name)
        with open(job_file_full, "w") as fd:
            fd.write(job_info_str)
            fd.close()

        #write version.xml file
        version_file_full = os.path.join(plat_dir, "version.xml")
        
        #create version string for any files (other than wrapper) for which the physical name is present
        version_files_str = ""
        if(len(plat_plus_job_app_files) > 0):
            for pf in plat_plus_job_app_files[0]["files"]: #accessing index 0, since each platform is mentioned only once
                print pf
                if(pf["physical_name"] != None):
                    print pf["physical_name"]
                    print pf["logical_name"]
                    version_files_str += (file_info%(pf["physical_name"], pf["logical_name"]))

        if(len(plat_plus_other_input_files) > 0):
            for pf in plat_plus_other_input_files[0]["files"]:
                if(pf["physical_name"] != None):
                    version_files_str += (file_info%(pf["physical_name"], pf["logical_name"]))

        for f in other_common_files: 
            version_files_str += (file_info%(f["physical_name"], f["logical_name"]))

        if(len(plat_plus_wrapper_file) > 0):
            version_str = version_info%(os.path.basename(plat_plus_wrapper_file[0]["filename"]), job_file_name, version_files_str)

        with open(version_file_full, "w") as fd:
            fd.write(version_str)
            fd.close()

    #update version.xml
    print("Update project.xml")
    if new_ver is "1.0":
    	app_info_str = app_info%(app_cfg["app_name"], app_cfg["app_user_friendly_name"])
    	with open(os.path.join(root_dir, "project.xml"), "r+") as fd:
        	lines = fd.readlines()
		strip_lines = [line.strip() for line in lines]
        	end_index = strip_lines.index('</boinc>')
        	lines.insert(end_index, app_info_str+'\n')
        	fd.seek(0)
        	fd.writelines(lines)
        	fd.close()

    print("Add application")
    saved_dir = os.getcwd()
    os.chdir(root_dir)
    xadd_cmd = "./bin/xadd"
    os.system(xadd_cmd)
    print("Update versions")
    update_versions_cmd = "./bin/update_versions"
    os.system(update_versions_cmd)
    os.chdir(saved_dir)
    
    if new_ver is "1.0":
        validator_name = "sample_trivial_validator_" + app_cfg["app_name"]
        shutil.copy2(os.path.join(root_dir, "bin", "sample_trivial_validator"), os.path.join(root_dir, "bin", validator_name))
        print("Update config.xml")
        #update config.xml to add a daemon to run the application
        app_daemon_str = app_daemon%(validator_name, app_cfg["app_name"])
        with open(os.path.join(root_dir, "config.xml"), "r+") as fd:
            lines = fd.readlines()
            strip_lines = [line.strip() for line in lines]
            end_index = strip_lines.index('</daemons>')
            lines.insert(end_index, app_daemon_str+'\n')
            fd.seek(0)
            fd.writelines(lines)
            fd.close()
	
    print("finished creating app %s"%(app_cfg["app_name"]))
    
    print("Restarting project")
    os.chdir(root_dir)
    os.system("./bin/stop") 
    os.system("./bin/start")
    
    os.chdir(saved_dir) 

if __name__ == "__main__":
    args = parse_arguments()
    create_app(args.app_cfg_file, args.platforms_supported)
