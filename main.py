import os, shutil, io, time, random
from matplotlib import pyplot as plt
import numpy as np
from PIL import Image

import raid6

if __name__ == "__main__":
    block_sizes = [8, 16, 32, 64, 128, 256, 512, 1024]
    disk_nums = [2, 3, 4, 5, 6, 7, 8]

    short_text_write_times = np.zeros((7, 8), dtype=float)
    long_text_write_times = np.zeros((7, 8), dtype=float)
    image_write_times = np.zeros((7, 8), dtype=float)

    short_text_retrieve_times = np.zeros((7, 8), dtype=float)
    long_text_retrieve_times = np.zeros((7, 8), dtype=float)
    image_retrieve_times = np.zeros((7, 8), dtype=float)

    reconstruct_times = np.zeros((3, 7, 8), dtype=float)

    start_time = 0.0

    results_path = "./results"
    if os.path.exists(results_path):
        shutil.rmtree(results_path)
    else:
        os.mkdir(results_path)

    dn = 0
    for disk_num in disk_nums:
        bs = 0
        for block_size in block_sizes:
            ### Initialize RAID6 ###
            print("--- Initializing RAID6 with {} disks and block size {} ---".format(disk_num+2, block_size))
            storage = raid6.RAID6('./nodes', disk_num, 2, block_size)
            files = []
            
            ### TEST FILE 1 ###
            print("Test 1: Short text file")
            file_name = "short_text.txt"
            files.append(file_name)
            with open(file_name, 'rb') as f:
                data = f.read()
                start_time = time.time()
                storage.write(file_name, data)
                wtime = time.time() - start_time
                short_text_write_times[dn, bs] = wtime * 1000.0
            
            # Retrieve stored file from storage
            start_time = time.time()
            result = storage.retrieve(file_name)
            wtime = time.time() - start_time
            short_text_retrieve_times[dn, bs] = wtime * 1000.0
            #print(result)

            # Simulate random node failures
            print("Simulating random disk failures...")
            fail_ids = random.sample(range(disk_num+2), 2)
            failed_node_block_1 = storage.fail_node(fail_ids[0])
            failed_node_block_2 = storage.fail_node(fail_ids[1])
            
            # Detect node failures and reconstruct the missing nodes
            print("Detecting disk failures and reconstructing missing data...")
            start_time = time.time()
            storage.handle_disk_failure()
            wtime = time.time() - start_time
            reconstruct_times[0, dn, bs] = wtime * 1000.0

            # Retrieve file to verify reconstructed contents
            print("Retrieving reconstructed file...\n--")
            result = storage.retrieve(file_name)
            #print(result)
            print("---------------------------")

            # ------------------------------------------------------------------------- #

            ### ADDING TEST FILE 2 TO STORAGE ###
            print("Test 2: Long text file")
            file_name = "long_text.txt"
            files.append(file_name)
            with open(file_name, 'rb') as f:
                data = f.read()
                start_time = time.time()
                storage.write(file_name, data)
                wtime = time.time() - start_time
                long_text_write_times[dn, bs] = wtime * 1000.0
            
            # Retrieve stored file from storage
            start_time = time.time()
            result = storage.retrieve(file_name)
            wtime = time.time() - start_time
            long_text_retrieve_times[dn, bs] = wtime * 1000.0
            #print(result)

            # Simulate random node failures
            print("Simulating random disk failures...")
            fail_ids = random.sample(range(disk_num+2), 2)
            failed_node_block_1 = storage.fail_node(fail_ids[0])
            failed_node_block_2 = storage.fail_node(fail_ids[1])
            
            # Detect node failures and reconstruct the missing nodes
            print("Detecting disk failures and reconstructing missing data...")
            start_time = time.time()
            storage.handle_disk_failure()
            wtime = time.time() - start_time
            reconstruct_times[1, dn, bs] = wtime * 1000.0

            # Retrieve files to verify reconstructed contents
            print("Retrieving reconstructed files...\n--")
            for file in files:
                result = storage.retrieve(file)
                #print(result)
                print("--")
            print("---------------------------")

            # ------------------------------------------------------------------------- #

            ### ADDING TEST FILE 3 TO STORAGE ###
            print("Test 2: Image file")
            file_name = "lena.jpg"
            files.append(file_name)
            with open(file_name, 'rb') as f:
                data = f.read()
                start_time = time.time()
                storage.write(file_name, data)
                wtime = time.time() - start_time
                image_write_times[dn, bs] = wtime * 1000.0
            
            # Retrieve stored file from storage
            start_time = time.time()
            result = storage.retrieve(file_name)
            wtime = time.time() - start_time
            image_retrieve_times[dn, bs] = wtime * 1000.0
            #image = Image.open(io.BytesIO(result))
            #image.show()

            # Simulate random node failures 
            print("Simulating random disk failures...")
            fail_ids = random.sample(range(disk_num+2), 2)
            failed_node_block_1 = storage.fail_node(fail_ids[0])
            failed_node_block_2 = storage.fail_node(fail_ids[1])
            
            # Detect node failures and reconstruct the missing nodes
            print("Detecting disk failures and reconstructing missing data...")
            start_time = time.time()
            storage.handle_disk_failure()
            wtime = time.time() - start_time
            reconstruct_times[2, dn, bs] = wtime * 1000.0

            # Retrieve files to verify reconstructed contents
            print("Retrieving reconstructed files...\n--")
            # Using this variable i to separate image file from the texts, since we don't have any file type detection in our implementation
            i = 0
            for file in files:
                result = storage.retrieve(file)
                # if i < 2:
                #     print(result)
                # else:
                #     image = Image.open(io.BytesIO(result))
                #     image.show()
                print("--")
                i += 1
            print("---------------------------")

            # Cleanup nodes
            shutil.rmtree('./nodes')

            bs += 1
        dn += 1

    ### Save the experimental results ###
    np.savetxt(os.path.join(results_path, "short_text_write_times.csv"), short_text_write_times, delimiter=",")
    np.savetxt(os.path.join(results_path, "long_text_write_times.csv"), long_text_write_times, delimiter=",")
    np.savetxt(os.path.join(results_path, "image_write_times.csv"), image_write_times, delimiter=",")

    np.savetxt(os.path.join(results_path, "short_text_retrieve_times.csv"), short_text_retrieve_times, delimiter=",")
    np.savetxt(os.path.join(results_path, "long_text_retrieve_times.csv"), long_text_retrieve_times, delimiter=",")
    np.savetxt(os.path.join(results_path, "image_retrieve_times.csv"), image_retrieve_times, delimiter=",")
    
    np.savetxt(os.path.join(results_path, "reconstruct_times_0.csv"), reconstruct_times[0, :, :], delimiter=",")
    np.savetxt(os.path.join(results_path, "reconstruct_times_1.csv"), reconstruct_times[1, :, :], delimiter=",")
    np.savetxt(os.path.join(results_path, "reconstruct_times_2.csv"), reconstruct_times[2, :, :], delimiter=",")
