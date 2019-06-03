import os
import shutil

filepath = os.path.dirname(os.path.realpath(__file__))
platforms_supported = ["x86_64-apple-darwin", "x86_64-pc-linux-gnu", "windows_x86_64"]

def test():
    #server_details_file = "server_auth_files/server_details"
    server_details_file = "../test_auth/server_details"
    os.system("python ../create_app/create_app_wrapper.py {} {} --platforms_supported {}".format('cfg/app_config.json', server_details_file, ' '.join(platforms_supported)))
    #os.system("python ../create_app/create_app.py {} --platforms_supported {}".format('cfg/app_config.json', ' '.join(platforms_supported)))

if __name__ == "__main__":
    test()

