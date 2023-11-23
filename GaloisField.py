import numpy as np

### This class defines the Galois field that is used for the RAID-6 implementation ###
class galoisfield(object):
    def __init__(self, k, m):
        '''inital setting
        field: GF(2^w), w=8
        primitive polynomial: x^8+x^4+x^3+x^2+1
        '''
        self.k = k
        self.m = m
        self.modulus = 0b100011101
        self.range = 1 << 8
        self.gflog = np.zeros((self.range,), dtype=int)
        self.gfilog = np.zeros((self.range,), dtype=int)
        self.vander = np.zeros((self.m, self.k), dtype=int)
        self.log_table()
        self.vander_matrix()

    ###
    #   Set up the Look-up-Table for logarithms
    ###
    def log_table(self):
        b = 1
        for log in range(self.range - 1):
            self.gflog[b] = log
            self.gfilog[log] = b
            b = b << 1
            if b & self.range:
                b = b ^ self.modulus

    ###
    #   Set up the Vandermonte matrix to be used later for calculating the parities
    ###
    def vander_matrix(self):
        for i in range(self.m):
            for j in range(self.k):
                self.vander[i][j] = self.power(j + 1, i)

    ###
    #   Addition operation in Galois field is XOR
    ###
    def add(self, a, b):
        '''Sum in Galosis Field
        '''
        sum = a ^ b
        return sum

    ###
    #   In this Galois field with characteristic of power of 2, addition and subtraction are the same 
    ###
    def sub(self, a, b):
        return self.add(a, b)

    ###
    #   Multiplication operation in Galois field
    ###
    def mult(self, a, b):
        sum_log = 0
        if a == 0 or b == 0:
            return 0
        sum_log = self.gflog[a] + self.gflog[b]
        if sum_log >= self.range - 1:
            sum_log -= self.range - 1
        return self.gfilog[sum_log]

    ###
    #   Division in Galois field
    ###
    def div(self, a, b):
        diff_log = 0
        if a == 0:
            return 0
        if b == 0:
            return -1
        diff_log = self.gflog[a] - self.gflog[b]
        if diff_log < 0:
            diff_log += self.range - 1
        return self.gfilog[diff_log]

    ###
    #   Exponent in Galois field
    ###
    def power(self, a, n):
        n %= self.range - 1
        res = 1
        while True:
            if n == 0:
                return res
            n -= 1
            res = self.mult(a, res)

    ###
    #   Inner product
    ###
    def dot(self, a, b):
        res = 0
        for i in range(len(a)):
            res = self.add(res, self.mult(a[i], b[i]))
        return res

    ###
    #   Matrix multiplication
    ###
    def matmul(self, a, b):
        res = np.zeros([a.shape[0], b.shape[1]], dtype=int)
        for i in range(res.shape[0]):
            for j in range(res.shape[1]):
                res[i][j] = self.dot(a[i, :], b[:, j])
        return res

    ###
    #   Matrix inversion
    ###
    def inverse(self, A):
        if A.shape[0] != A.shape[1]:
            A_T = np.transpose(A)
            A_ = self.matmul(A_T, A)
        else:
            A_ = A
        A_ = np.concatenate((A_, np.eye(A_.shape[0], dtype=int)), axis=1)
        dim = A_.shape[0]
        for i in range(dim):
            if not A_[i, i]:
                for k in range(i + 1, dim):
                    if A_[k, i]:
                        break
                A_[i, :] = list(map(self.add, A_[i, :], A_[k, :]))
            A_[i, :] = list(map(self.div, A_[i, :], [A_[i, i]] * len(A_[i, :])))
            for j in range(i + 1, dim):
                A_[j, :] = self.add(A_[j, :],
                                    list(map(self.mult, A_[i, :], [self.div(A_[j, i], A_[i, i])] * len(A_[i, :]))))
        for i in reversed(range(dim)):
            for j in range(i):
                A_[j, :] = self.add(A_[j, :], list(map(self.mult, A_[i, :], [A_[j, i]] * len(A_[i, :]))))
        A_inverse = A_[:, dim:2 * dim]
        if A.shape[0] != A.shape[1]:
            A_inverse = self.matmul(A_inverse, A_T)

        return A_inverse
