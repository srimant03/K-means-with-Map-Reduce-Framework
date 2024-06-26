import grpc
import os
from concurrent import futures
import numpy as np
import logging

import kmeans_pb2
import kmeans_pb2_grpc

class Mapper(kmeans_pb2_grpc.KMeansClusterServicer):
    def __init__(self, data_file='points.txt'):
        self.data_file = data_file

    def read_data_segment(self, start_index, end_index):
        data = []
        with open(self.data_file, 'r') as file:
            for i, line in enumerate(file):
                if start_index <= i < end_index:
                    data.append(list(map(float, line.strip().split())))
        return data

    def calculate_distance(self, point, centroid):
        return np.sqrt(sum((p - c) ** 2 for p, c in zip(point, centroid)))

    def map_function(self, input_split, centroids):
        result = []
        for point in input_split:
            min_dist = float('inf')
            nearest_centroid = None
            for centroid in centroids:
                dist = self.calculate_distance(point, centroid)
                if dist < min_dist:
                    min_dist = dist
                    nearest_centroid = centroid
            result.append((nearest_centroid, point))
        return result

    def partition(self, mapped_values, num_reducers):
        partitions = [[] for _ in range(num_reducers)]
        for centroid, point in mapped_values:
            partitions[hash(tuple(centroid)) % num_reducers].append((centroid, point))
        return partitions

    def SendDataToMapper(self, request, context):
        centroids = [list(centroid.coordinates) for centroid in request.centroids]
        input_split = self.read_data_segment(request.range_start, request.range_end)
        mapped_values = self.map_function(input_split, centroids)
        partitions = self.partition(mapped_values, request.num_reducers)

        mapper_dir = f'mapper_{request.mapper_id}'
        if not os.path.exists(mapper_dir):
            os.makedirs(mapper_dir)

        for i, partition in enumerate(partitions):
            with open(os.path.join(mapper_dir, f'partition_{i}.txt'), 'w') as f:
                for centroid, point in partition:
                    f.write(f"{centroid} {point}\n")

        return kmeans_pb2.MapperResponse(mapper_id=request.mapper_id, status="SUCCESS")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    kmeans_pb2_grpc.add_KMeansClusterServicer_to_server(Mapper(), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    logging.basicConfig(filename='mapper_log.txt', level=logging.INFO)
    serve()
