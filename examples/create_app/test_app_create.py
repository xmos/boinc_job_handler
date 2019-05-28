import os
import shutil

filepath = os.path.dirname(os.path.realpath(__file__))
app_cfg_file = "app_config.cfg"
platforms_supported = ["x86_64-apple-darwin", "x86_64-pc-linux-gnu", "windows_x86_64"]
def test():
    app_cfg_file_temp = "app_cfg_temp.cfg"
    boinc_wrapper_path = os.path.join(filepath, "wrappers")
    with open(app_cfg_file, "r") as fd:
	lines = fd.readlines()
	strip_lines = [line.strip() for line in lines]
	end_index = strip_lines.index('[boinc_python_wrappers]')
        num_platforms = len(platforms_supported)
        for count in range(num_platforms):
            l = strip_lines[end_index+count+1].split(":")
            lines[end_index+count+1] = l[0]+":"+os.path.join(boinc_wrapper_path,l[1]) + "\n"
        
        with open(app_cfg_file_temp, "w") as fd1:
            fd1.writelines(lines)
            fd1.close()
	fd.close()
    
    #python ../../create_app/create_app_wrapper.py app_config.cfg --platforms_supported x86_64-apple-darwin x86_64-pc-linux-gnu
    os.system("python ../../create_app/create_app_wrapper.py {} --platforms_supported {}".format(app_cfg_file_temp, ' '.join(platforms_supported)))

    os.remove(app_cfg_file_temp)


if __name__ == "__main__":
    test()

