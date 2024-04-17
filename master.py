import grpc
from concurrent import futures
import random
import logging
import numpy as np
import os
import threading
import time
import subprocess

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
    
    def spawn_and_grpc_to_mapper(self, idx, range_start, range_end, mapper_responses):
        #command = f'python3 mapper.py --port={5000 + idx}'
        #print(f"Command: {command}")
        #command = f'python3 mapper.py {5002 + idx}'
        #os.system(command)                
        '''request = kmeans_pb2.MapperRequest()
        request.mapper_id = idx
        request.range_start = range_start
        request.range_end = range_end
        request.num_red = self.r
        for centroid in self.centroids:
            centroid_message = request.centroids.add()
            centroid_message.coordinates.extend(centroid)
        
        print(request.centroids)

        response = self.mapper_stub.SendDataToMapper(request)
        print(f"Mapper {idx} response: {response.status}")
        mapper_responses.append(response)
        logging.info(f"Mapper {idx} response: {response.status}")'''

        port = 5001 + idx
        channel = grpc.insecure_channel(f'localhost:{port}')
        mapper_stub = kmeans_pb2_grpc.KMeansClusterStub(channel)

        request = kmeans_pb2.MapperRequest()
        request.mapper_id = idx
        request.range_start = range_start
        request.range_end = range_end
        request.num_red = self.r
        for centroid in self.centroids:
            centroid_message = request.centroids.add()
            centroid_message.coordinates.extend(centroid)

        response = mapper_stub.SendDataToMapper(request)
        mapper_responses.append(response)
        logging.info(f"Mapper {idx} response: {response.status}")
    
    def spawn_and_grpc_to_reducer(self, idx, reducer_responses):
        #command = 'python3 reducer.py'
        #print(f"Command: {command}")
        #command = f'python3 reducer.py {6000 + idx}'
        #os.system(command)               
        #response = self.reducer_stub.ProcessDataForReducer(kmeans_pb2.ReducerRequest(reducer_id=idx, num_mappers=self.m))
        #print(f"Reducer {idx} response: {response.status}")
        #reducer_responses.append(response)
        #logging.info(f"Reducer {idx} response: {response.status}")
        '''print("reducer",idx)
        response = self.reducer_stub.ProcessDataForReducer(kmeans_pb2.ReducerRequest(reducer_id=idx, num_mappers=self.m))
        print(f"Reducer {idx} response: {response.status}")
        reducer_responses.append(response)
        print(response.new_centroids)
        logging.info(f"Reducer {idx} response: {response.status}")'''

        port = 6001 + idx
        channel = grpc.insecure_channel(f'localhost:{port}')
        reducer_stub = kmeans_pb2_grpc.KMeansClusterStub(channel)
        response = reducer_stub.ProcessDataForReducer(kmeans_pb2.ReducerRequest(reducer_id=idx, num_mappers=self.m))
        reducer_responses.append(response)
        logging.info(f"Reducer {idx} response: {response.status}")
           
    def run(self):
        logging.basicConfig(filename='dump.txt', level=logging.DEBUG)
        logging.info(f"Initial centroids: {self.centroids}")

        for iteration in range(self.max_iter):
            logging.info(f"Starting iteration {iteration + 1}")
            mapper_responses = []
            t = []

            for idx in range(self.m):
                port = 5001 + idx  
                command = ['python3', 'mapper.py', str(port)]  
                subprocess.Popen(command)  
                logging.info(f"Mapper {idx} spawned on port {port}")
            time.sleep(1)

            for idx in range(self.m):
                range_start = idx * (len(self.input) // self.m)
                range_end = (idx + 1) * (len(self.input) // self.m) if idx < self.m - 1 else len(self.input)
                print(f"Range start: {range_start}, Range end: {range_end}")
                x=threading.Thread(target=self.spawn_and_grpc_to_mapper, args=(idx, range_start, range_end, mapper_responses))
                x.start()
                t.append(x)

            for i in t:
                i.join()
                      
            if all(response.status == "SUCCESS" for response in mapper_responses):
                reducer_responses = []
                new_centroids = []
                t1 = []

                for idx in range(self.r):
                    port = 6001 + idx  
                    command = ['python3', 'reducer.py', str(port)]  
                    subprocess.Popen(command)  
                    logging.info(f"Reducer {idx} spawned on port {port}")
                time.sleep(1)
                    
                for idx in range(self.r):
                    '''request = kmeans_pb2.ReducerRequest(reducer_id=idx, num_mappers=self.m)
                    response = self.reducer_stub.ProcessDataForReducer(request)
                    reducer_responses.append(response)
                    new_centroids.extend(response.new_centroids)'''
                    print("threading",idx)
                    x=threading.Thread(target=self.spawn_and_grpc_to_reducer, args=(idx, reducer_responses))
                    x.start()
                    t1.append(x)

                for i in t1:
                    i.join()
            
            for response in reducer_responses:
                new_centroids.extend(response.new_centroids)

            new_centro = []

            for cen in new_centroids:
                l=[]
                for coord in cen.coordinates:
                    l.append(coord)
                new_centro.append(l)
                        
            logging.info(f"New centroids: {new_centro}")
            
            if self.has_converged(self.centroids, new_centro):
                logging.info("Convergence reached.")
                break
            else:
                self.centroids = new_centro
                logging.info(f"Updated centroids: {self.centroids}")
            
            #close all mapper and reducer processes
            os.system('pkill -f mapper.py')
            os.system('pkill -f reducer.py')

    '''def spawn_mapper_Reducer(self):
        for i in range(self.m):
            command=f'python mapper.py --mapper_id {i}'
            threading.Thread(target=lambda:os.system(command)).start()
        for i in range(self.r):
            command=f'python reducer.py --reducer_id {i}'
            threading.Thread(target=lambda:os.system(command)).start()'''
    

    def serve(self):
        run_thread = threading.Thread(target=self.run)
        run_thread.start()
        #server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        #kmeans_pb2_grpc.add_KMeansClusterServicer_to_server(self, server)
        #server.add_insecure_port('[::]:5000')
        #server.start()
        #server.wait_for_termination()

if __name__ == '__main__':
    M = input("Enter the number of mappers: ")
    R = input("Enter the number of reducers: ")
    K = input("Enter the number of clusters: ")
    iter = input("Enter the number of iterations: ")
    dataset_location = 'data.txt'
    master = Master(M, R, K, iter, dataset_location)
    master.serve()
