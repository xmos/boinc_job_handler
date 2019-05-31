import os

platforms_supported=["x86_64-apple-darwin", "x86_64-pc-linux-gnu"]

def test():
    test_auth_file = "server_auth_files/server_details"
    os.system("python ../batch_process/batch_process.py cfg/app_config.json cfg/batch_config.json {} --platforms_supported {}".format(test_auth_file, ' '.join(platforms_supported)))

if __name__ == "__main__":
    test()

