import os
import subprocess
import shutil
import json
import argparse

temp_dir="temp_create_app_files"
app_create_script = "create_app.py"

filepath = os.path.dirname(os.path.realpath(__file__))

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("app_cfg_file", help="app config file",)
    parser.add_argument("server_details_file", help="file containing server name and project name",)
    parser.add_argument("--platforms_supported", nargs="*",type=str,default=[])
    parser.parse_args()
    args = parser.parse_args()
    return args

def get_server_details(server_file):
    with open(server_file, "r") as f:
        boinc_server_name = (f.readline()).strip()
        boinc_project_dir = (f.readline()).strip()
    return [boinc_server_name, boinc_project_dir]

def run_app_create_wrapper(app_cfg_file, server_details_file, platforms_supported): 
    assert(len(platforms_supported) > 0),"no platforms specified. Provide --platform_supported argument on the command line"

    #create a list of all files that need to be copied on the server
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)

    shutil.copy2(os.path.join(filepath, app_create_script), temp_dir)

    fapp = open(app_cfg_file, "r")
    app_config = json.load(fapp)
    fapp.close()

    for pf in app_config["python_wrapper_file"]:
        shutil.copy2(pf["filename"], temp_dir)
        pf["filename"] = os.path.basename(pf["filename"])

    for app_files in app_config["job_application_files"]:
        for f in app_files["files"]:
            if f["physical_name"] != None:
                shutil.copy2(f["physical_name"], temp_dir)
                f["physical_name"] = os.path.basename(f["physical_name"])

    for app_files in app_config["other_platform_dependent_input_files"]:
        for f in app_files["files"]:
            if f["physical_name"] != None:
                shutil.copy2(f["physical_name"], temp_dir)
                f["physical_name"] = os.path.basename(f["physical_name"])
    
    for common_file in app_config["other_common_input_files"]:
        if common_file["physical_name"] != None:
            shutil.copy2(common_file["physical_name"], temp_dir)
            common_file["physical_name"] = os.path.basename(common_file["physical_name"])

    app_cfg_file_temp = os.path.join(temp_dir, os.path.basename(app_cfg_file))
    #print app_config
    #y = json.dumps(app_config)
    with open(app_cfg_file_temp, "w") as fd:
        json.dump(app_config, fd)
        fd.close()
    
    boinc_server_name, boinc_project_dir = get_server_details(server_details_file)

    print boinc_server_name
    print boinc_project_dir
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
    run_app_create_wrapper(args.app_cfg_file, args.server_details_file, args.platforms_supported)
