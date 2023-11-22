import raid6
from random import randrange

if __name__ == "__main__":
    storage = raid6.RAID6('./nodes', 4, 2, 8)
    files = ['test_small.txt', 'test_middle.txt']

    for file_name in files:
        with open(file_name, 'rb') as f:
            data = f.read()
            storage.write(file_name, data)

        result = storage.retrieve(file_name)
        print(result)

    # Random Disk Failure and Detection
    print("\nSimulating Disk Failures and Detecting...")
    failed_node_block_1 = storage.fail_disk(node_id=randrange(0, 6), block_id=randrange(0, 16))
    failed_node_block_2 = storage.fail_disk(node_id=randrange(0, 6), block_id=randrange(0, 16))

    # Detect which disks have failed
    fail_ids = storage.detect_failure()
    print("Detected Failed Disks:", fail_ids)
