import math
import os
import shutil
from pathlib import Path

import numpy as np

from GaloisField import GaloisField


class RAID6():
    def __init__(self, path, k, m, block_size):
        '''

        :param k: number of storage nodes
        :param m: number of parity nodes
        '''
        self.k = k
        self.m = m
        self.path = path
        self.block_size = block_size
        self.gf = GaloisField(k=self.k, m=self.m)
        self.stripe_size = self.block_size * k
        self.cur_stripe_index = 0
        self.obj_stripe_index = {}
        if os.path.isdir(path) and not os.path.islink(path):
            shutil.rmtree(path)
        elif os.path.exists(path):
            os.remove(path)

        os.mkdir(self.path)
        for i in range(k + m):
            Path(self.path+'/node{}'.format(i)).touch()

    def write(self, data):
        data = bytearray(data)
        splits = self.split_by_disk(data)
        splits = self.append_parities(splits)

        for i in range(len(splits)):
            with open(self.path+'/node{}'.format(i), 'wb') as f:
                split_bytes = bytes(splits[i,:].tolist())
                f.write(split_bytes)
                f.flush()
                os.fsync(f.fileno())

    def split_by_disk(self, data):
        data_size = len(data)

        stripe_num = math.ceil(data_size / self.stripe_size)

        data += b'\0' * (stripe_num * self.stripe_size - data_size)

        data = np.asarray(data, dtype=int)
        splits = data.reshape(self.k, self.block_size * stripe_num)
        return splits


    def append_parities(self, splits):
        parities = self.gf.matmul(self.gf.vander, splits)
        return np.concatenate([parities, splits], axis=0)


    def retrieve(self):
        splits = None

        with open(self.path+'/node0', 'rb') as f:
            split = np.asarray(bytearray(f.read()))
            split = split.reshape(1, len(split))
            splits = split
        i = 1
        while i < self.k:
            with open(self.path+'/node{}'.format(i), 'rb') as f:
                split = np.asarray(bytearray(f.read()))
                split = split.reshape(1, len(split))
                splits = np.concatenate([splits, split], axis=0)
            i += 1

        data = splits.flatten().tobytes()
        return data



