import os
import subprocess
import shutil
import argparse
import configparser

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

def parse_app_cfg_file(cfg_file, platforms_supported):
    config = configparser.ConfigParser()
    config.read(cfg_file)
    sections = config.sections()
    app_name = config.get('app_params', 'app_name')
    app_desc = config.get('app_params', 'app_desc')
    input_file = config.get('app_params', 'input_file_logical_name')
    output_files = config.get('app_params', 'output_files_logical_name')

    executable_logical_names = {} #dictionary containing executable local names
    wrapper_names = {}

    for platform in platforms_supported:
        executable = None
        try:
            executable = config.get('executable_logical_names', platform)
        except:
            pass
        if executable is not None:
            executable_logical_names[platform] = executable

        wrapper = None
        try:
            wrapper = config.get('boinc_python_wrappers', platform)
        except:
            pass
        if wrapper is not None:
            wrapper_names[platform] = wrapper


    output_files_list = output_files.split()
    return app_config_params(app_name, app_desc, input_file, output_files_list, executable_logical_names, wrapper_names)
    test_params = parse_test_params(config, 'test_params')
    return test_params


def create_app(app_cfg_file, platforms_supported):
    job_info = """
<job_desc>
    <task>
        <application>%s</application>
    </task>
</job_desc>
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
</version>
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
    path_to_app = os.path.join(root_dir+"apps", app_cfg.app_name)
    new_ver = "1.0"
    if os.path.exists(path_to_app):
        imm_subdirs = next(os.walk(path_to_app))[1] #get immediate subdirectories to the apps directory
        existing_vers = [float(y) for y in imm_subdirs]
        new_ver = str(max(existing_vers) + 1.0)
    
    print new_ver	
    version_dir = os.path.join(path_to_app, new_ver)
    os.makedirs(version_dir)
    platforms = app_cfg.executable_logical_names_dict.keys()
    for p in platforms:
        plat_dir = os.path.join(version_dir, p)
        os.makedirs(plat_dir)
        #copy the wrapper file
        shutil.copy2(app_cfg.wrapper_names_dict[p], plat_dir)
        job_desc_str = job_info%(app_cfg.executable_logical_names_dict[p])
        #with open("create_file_name_of_job_file") as 
        job_file_name = app_cfg.app_name + "_job" + "_" + new_ver + "_" + p
        job_file_full = os.path.join(plat_dir, job_file_name)
        with open(job_file_full, "w") as fd:
            fd.write(job_desc_str)
            fd.close()
        version_file_full = os.path.join(plat_dir, "version.xml")
        version_str = version_info%(os.path.basename(app_cfg.wrapper_names_dict[p]), job_file_name)
        with open(version_file_full, "w") as fd:
            fd.write(version_str)
            fd.close()

    #update version.xml
    print("Update version.xml")
    if new_ver is "1.0":
    	app_info_str = app_info%(app_cfg.app_name, app_cfg.app_desc)
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
        validator_name = "sample_trivial_validator_" + app_cfg.app_name
        shutil.copy2(os.path.join(root_dir, "bin", "sample_trivial_validator"), os.path.join(root_dir, "bin", validator_name))
        print("Update config.xml")
        #update config.xml to add a daemon to run the application
        app_daemon_str = app_daemon%(validator_name, app_cfg.app_name)
        with open(os.path.join(root_dir, "config.xml"), "r+") as fd:
            lines = fd.readlines()
            strip_lines = [line.strip() for line in lines]
            end_index = strip_lines.index('</daemons>')
            lines.insert(end_index, app_daemon_str+'\n')
            fd.seek(0)
            fd.writelines(lines)
            fd.close()
	
    print("finished creating app %s"%(app_cfg.app_name))
    
    print("Restarting project")
    os.chdir(root_dir)
    os.system("./bin/stop") 
    os.system("./bin/start")
    
    os.chdir(saved_dir) 

if __name__ == "__main__":
    args = parse_arguments()
    create_app(args.app_cfg_file, args.platforms_supported)
