import grpc
from concurrent import futures
import random
import logging
import numpy as np

import kmeans_pb2
import kmeans_pb2_grpc

class Master(kmeans_pb2_grpc.KMeansClusterServicer):
    def __init__(self, M, R, K, iter, dataset_location):
        self.m = int(M)
        self.r = int(R)
        self.k = int(K)
        self.max_iter = int(iter)
        self.dataset_location = dataset_location
        self.input = self.read_input()
        self.centroids = self.assign_k_centroids()

    def read_input(self):
        with open(self.dataset_location, 'r') as file:
            data = [list(map(float, line.strip().split(','))) for line in file.readlines()]
        return data

    def assign_k_centroids(self):
        return random.sample(self.input, self.k)

    def has_converged(self, old_centroids, new_centroids, threshold=0.0001):
        for old, new in zip(old_centroids, new_centroids):
            if np.linalg.norm(np.array(old) - np.array(new)) > threshold:
                return False
        return True

    def run(self):
        logging.basicConfig(filename='dump.txt', level=logging.DEBUG)
        logging.info(f"Initial centroids: {self.centroids}")

        for iteration in range(self.max_iter):
            logging.info(f"Starting iteration {iteration + 1}")
            mapper_responses = []
            for idx in range(self.m):
                range_start = idx * (len(self.input) // self.m)
                range_end = (idx + 1) * (len(self.input) // self.m) if idx < self.m - 1 else len(self.input)
                request = kmeans_pb2.MapperRequest(mapper_id=idx, centroids=self.centroids, range_start=range_start, range_end=range_end)
                response = self.mapper_stub.SendDataToMapper(request)
                mapper_responses.append(response)
                logging.info(f"Mapper {idx} response: {response.status}")

            if all(response.status == "SUCCESS" for response in mapper_responses):
                reducer_responses = []
                new_centroids = []
                for idx in range(self.r):
                    request = kmeans_pb2.ReducerRequest(reducer_id=idx)
                    response = self.reducer_stub.ProcessDataForReducer(request)
                    reducer_responses.append(response)
                    new_centroids.extend(response.new_centroids)

            if self.has_converged(self.centroids, new_centroids):
                logging.info("Convergence reached.")
                break
            else:
                self.centroids = new_centroids
                logging.info(f"Updated centroids: {self.centroids}")

    def spawn_mapper_Reducer(self):
        for i in range(self.m):
            command=f'python mapper.py --mapper_id {i}'
            threading.Thread(target=lambda:os.system(command)).start()
        for i in range(self.r):
            command=f'python reducer.py --reducer_id {i}'
            threading.Thread(target=lambda:os.system(command)).start()
    

    def serve(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        kmeans_pb2_grpc.add_KMeansClusterServicer_to_server(self, server)
        self.mapper_stub = kmeans_pb2_grpc.KMeansClusterStub(grpc.insecure_channel('localhost:50052'))
        self.reducer_stub = kmeans_pb2_grpc.KMeansClusterStub(grpc.insecure_channel('localhost:50053'))
        server.add_insecure_port('[::]:5001')
        server.start()
        server.wait_for_termination()

if __name__ == '__main__':
    M = input("Enter the number of mappers: ")
    R = input("Enter the number of reducers: ")
    K = input("Enter the number of clusters: ")
    iter = input("Enter the number of iterations: ")
    dataset_location = 'data.txt'
    master = Master(M, R, K, iter, dataset_location)
    master.serve()
