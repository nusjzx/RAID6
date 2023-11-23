import random
import time
import raid6
from matplotlib import pyplot as plt
from PIL import Image
import io

if __name__ == "__main__":
    # Initialize RAID6 with 6 nodes/disks (2 parities) and block size of 8 bytes
    storage = raid6.RAID6('./nodes', 4, 2, 16)
    # This class supports any other configurations too, for example:
    # storage = raid6.RAID6('./nodes', 6, 2, 32)
    files = []
    
    ### TEST FILE 1
    print("Test 1: Short text file")
    file_name = "short_text.txt"
    files.append(file_name)
    with open(file_name, 'rb') as f:
        data = f.read()
        storage.write(file_name, data)
    
    result = storage.retrieve(file_name)
    print(result)

    # Random Disk Failures and Detection
    print("Simulating random disk failures...")
    fail_ids = random.sample(range(6), 2)
    failed_node_block_1 = storage.fail_node(fail_ids[0])
    failed_node_block_2 = storage.fail_node(fail_ids[1])
    
    print("Detecting disk failures and reconstructing missing data...")
    storage.handle_disk_failure()

    # Retrieve file to verify reconstructed contents
    print("Retrieving reconstructed file...\n--")
    result = storage.retrieve(file_name)
    print(result)
    print("---------------------------")

    # ------------------------------------------------------------------------- #

    ### ADDING TEST FILE 2 TO STORAGE
    print("Test 2: Long text file")
    file_name = "long_text.txt"
    files.append(file_name)
    with open(file_name, 'rb') as f:
        data = f.read()
        storage.write(file_name, data)
    
    result = storage.retrieve(file_name)
    print(result)

    # Random Disk Failures and Detection
    print("Simulating random disk failures...")
    fail_ids = random.sample(range(6), 2)
    failed_node_block_1 = storage.fail_node(fail_ids[0])
    failed_node_block_2 = storage.fail_node(fail_ids[1])
    
    print("Detecting disk failures and reconstructing missing data...")
    storage.handle_disk_failure()

    # Retrieve files to verify reconstructed contents
    print("Retrieving reconstructed files...\n--")
    for file in files:
        result = storage.retrieve(file)
        print(result)
        print("--")
    print("---------------------------")

    # ------------------------------------------------------------------------- #

    ### ADDING TEST FILE 3 TO STORAGE
    print("Test 2: Image file")
    file_name = "lena.jpg"
    files.append(file_name)
    with open(file_name, 'rb') as f:
        data = f.read()
        storage.write(file_name, data)
    
    result = storage.retrieve(file_name)
    image = Image.open(io.BytesIO(result))
    image.show()

    # Random Disk Failures and Detection
    print("Simulating random disk failures...")
    fail_ids = random.sample(range(6), 2)
    failed_node_block_1 = storage.fail_node(fail_ids[0])
    failed_node_block_2 = storage.fail_node(fail_ids[1])
    
    print("Detecting disk failures and reconstructing missing data...")
    storage.handle_disk_failure()

    # Retrieve files to verify reconstructed contents
    print("Retrieving reconstructed files...\n--")
    # Using this variable i to separate image file from the texts, since we don't have any file type detection in our implementation
    i = 0
    for file in files:
        result = storage.retrieve(file)
        if i < 2:
            print(result)
        else:
            result = storage.retrieve(file)
            image = Image.open(io.BytesIO(result))
            image.show()
        print("--")
        i += 1
    print("---------------------------")
