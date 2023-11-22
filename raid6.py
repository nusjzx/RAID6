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

    def write(self, name, data):
        data = bytearray(data)
        splits, stripe_num= self.split_by_disk(data)
        self.obj_stripe_index[name] = [self.cur_stripe_index, stripe_num]
        self.cur_stripe_index = self.cur_stripe_index + stripe_num
        splits = self.append_parities(splits)

        for i in range(len(splits)):
            with open(self.path+'/node{}'.format(i), 'ab') as f:
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
        return splits, stripe_num


    def append_parities(self, splits):
        parities = self.gf.matmul(self.gf.vander, splits)
        return np.concatenate([splits, parities], axis=0)


    def retrieve(self, name):
        splits = []

        stripe_index, stripe_num = self.obj_stripe_index[name]

        for i in range(self.k):
            with open(self.path+'/node{}'.format(i), 'rb') as f:
                f.seek(stripe_index * self.block_size)
                split = np.asarray(bytearray(f.read(stripe_num * self.block_size)))
                split = split.reshape(1, len(split))
                splits.append(split)

        data_arr = np.concatenate(splits, axis=0).flatten()
        data = np.trim_zeros(data_arr, 'b').tobytes()
        return data
