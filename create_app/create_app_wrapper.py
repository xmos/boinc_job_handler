import os
import subprocess
import shutil
from create_app import app_config_params, parse_app_cfg_file
from create_app import parse_arguments

temp_dir="temp_create_app_files"
app_create_script = "create_app.py"
#boinc_server_name = "boincadm@boinc-server.xmos.com"
#boinc_project_dir = "/home/boincadm/projects/hellopython"
boinc_server_name = "boincadm@srv-bri-grid0"
boinc_project_dir = "/home/boincadm/projects/test_new"

filepath = os.path.dirname(os.path.realpath(__file__))

def run_app_create_wrapper(app_cfg_file, platforms_supported): 
    print filepath
    assert(len(platforms_supported) > 0),"no platforms specified. Provide --platform_supported argument on the command line"
    app_cfg = parse_app_cfg_file(app_cfg_file, platforms_supported)
    #create a list of all files that need to be copied on the server
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)

    for wrapper_file in app_cfg.wrapper_names_dict.values():
        shutil.copy2(wrapper_file, temp_dir)

    shutil.copy2(os.path.join(filepath, app_create_script), temp_dir)
    app_cfg_file_temp = os.path.join(temp_dir, os.path.basename(app_cfg_file))

    #update the cfg file to have different paths for the wrappers since they're copied on the server in a different directory
    with open(app_cfg_file, "r") as fd:
	lines = fd.readlines()
	strip_lines = [line.strip() for line in lines]
	end_index = strip_lines.index('[boinc_python_wrappers]')
        num_platforms = len(platforms_supported)
        for count in range(num_platforms):
            l = strip_lines[end_index+count+1].split(":")
            lines[end_index+count+1] = l[0]+":"+os.path.basename(l[1]) + "\n"
        
        with open(app_cfg_file_temp, "w") as fd1:
            fd1.writelines(lines)
            fd1.close()
	fd.close()

    #copy temp_dir on the server
    os.system("scp -p -r {} {}:{}".format(temp_dir, boinc_server_name, boinc_project_dir))
    
    #execute the app_create_script on the server
    platforms_supported_str = ' '.join(platforms_supported) 
    app_create_exec_dir = os.path.join(boinc_project_dir, temp_dir)
    os.system("ssh {} \"cd {} ; python {} {} --platforms_supported {}\"".format(boinc_server_name, app_create_exec_dir, app_create_script, os.path.basename(app_cfg_file_temp), platforms_supported_str))

    #delete the temp_dir created on the server
    os.system("ssh {} \"cd {} ; rm -rf {}\"".format(boinc_server_name, boinc_project_dir, temp_dir))
if __name__ == "__main__":
    args = parse_arguments()
    run_app_create_wrapper(args.app_cfg_file, args.platforms_supported)
