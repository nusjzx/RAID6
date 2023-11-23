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
            os.mkdir(self.path+'/node{}'.format(i))

    def write(self, name, data):
        data = bytearray(data)
        splits, stripe_num= self.split_by_block(data)
        self.obj_stripe_index[name] = [self.cur_stripe_index, stripe_num]
        splits = self.append_parities(splits)

        blocks = splits.reshape(self.k + self.m, stripe_num, self.block_size)


        for j in range(len(blocks[0])):
            for i in range(len(blocks)):
                stripe_i = self.cur_stripe_index + j
                # for every stripe, we roll the stripe by stripe_i
                roll_i = (i - stripe_i) % (self.k + self.m)
                with open(self.path+'/node{}'.format(roll_i) + '/block{}'.format(stripe_i), 'wb') as f:
                    block_bytes = bytes(blocks[i,j].tolist())
                    f.write(block_bytes)
                    f.flush()
                    os.fsync(f.fileno())
        self.cur_stripe_index = self.cur_stripe_index + stripe_num

    def split_by_block(self, data):
        data_size = len(data)

        stripe_num = math.ceil(data_size / self.stripe_size)

        data += b'\0' * (stripe_num * self.stripe_size - data_size)

        data = np.asarray(data, dtype=np.uint8)
        splits = data.reshape(self.k, stripe_num * self.block_size)
        return splits, stripe_num


    def append_parities(self, splits):
        parities = self.gf.matmul(self.gf.vander, splits)
        return np.concatenate([splits, parities], axis=0)


    def retrieve(self, name):
        splits = []
        stripe_index, stripe_num = self.obj_stripe_index[name]
        for i in range(self.k + self.m):
            split = np.array([], dtype=np.uint8)
            for j in range(stripe_num):
                stripe_i = stripe_index + j
                roll_i = (i - stripe_i) % (self.k + self.m)
                with open(self.path + '/node{}'.format(roll_i)+ '/block{}'.format(stripe_i), 'rb') as f:
                    block = np.asarray(bytearray(f.read()),dtype=np.uint8)
                    split = np.concatenate([split, block])
            split = split.reshape(1, len(split))
            splits.append(split)

        data = np.concatenate(splits, axis=0)[:self.k,]
        trimmed_data = np.trim_zeros(data.flatten(), 'b')
        return trimmed_data.tobytes()

    
    def fail_node(self, node_id):
        # introduce node and block failure by deleting the block file
        node_path = os.path.join(self.path, 'node{}'.format(node_id))
        if os.path.exists(node_path):
            shutil.rmtree(node_path)
            os.mkdir(node_path)
            print("Node {} failed".format(node_id))
            return node_id
        else:
            print("Node {} not found".format(node_id))
            return None

    
    def handle_disk_failure(self):
        for stripe_i in range(self.cur_stripe_index):
            normal_data = []
            failed_i = []
            # Check for missing data blocks (failures)
            for i in range(self.k + self.m):
                roll_i = (i - stripe_i) % (self.k + self.m)
                file_name = os.path.join(self.path, 'node{}'.format(roll_i), 'block{}'.format(stripe_i))
                if not os.path.exists(file_name):
                    #print("Detected failure: block {} in Node {}".format(stripe_i, roll_i))
                    failed_i.append(i)
                else:
                    with open(self.path + '/node{}'.format(roll_i) + '/block{}'.format(stripe_i), 'rb') as f:
                        block = np.asarray(bytearray(f.read()), dtype=np.uint8)
                        normal_data.append(block)
            data_left = np.concatenate([normal_data], axis=0)

            ### Recover lost node data ###
            # Concatenate identity matrix with Vandermonte matrix
            generator = np.concatenate([np.identity(self.k, dtype=np.uint8), self.gf.vander], axis=0)
            # Remove the rows corresponding to failed nodes
            generator_left = np.delete(generator, obj=failed_i, axis=0)

            # Invert the square matrix to solve for the missing node data
            data_result = self.gf.matmul(self.gf.inverse(generator_left), data_left)
            parity_result = self.gf.matmul(self.gf.vander, data_result)

            recovered_result = np.concatenate([data_result, parity_result], axis=0)

            for i in failed_i:
                roll_i = (i - stripe_i) % (self.k + self.m)
                with open(self.path + '/node{}'.format(roll_i) + '/block{}'.format(stripe_i), 'wb') as f:
                    block_bytes = bytes(recovered_result[i].tolist())
                    f.write(block_bytes)
                    f.flush()
                    os.fsync(f.fileno())
        return
