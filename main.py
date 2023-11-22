import raid6


if __name__ == "__main__":
    storage = raid6.RAID6('./nodes', 4, 2, 8)

    with open('test_small.txt', 'rb') as f:
        data = f.read()
        storage.write(data)

    result = storage.retrieve()
    print(result)