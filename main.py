import raid6


if __name__ == "__main__":
    storage = raid6.RAID6('./nodes', 4, 2, 8)
    files = ['test_small.txt', 'test_middle.txt']

    for file_name in files:
        with open(file_name, 'rb') as f:
            data = f.read()
            storage.write(file_name, data)

        result = storage.retrieve(file_name)
        print(result)