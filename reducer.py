import os
import grpc
from concurrent import futures
import numpy as np
import logging
import re
import sys
import kmeans_pb2
import kmeans_pb2_grpc

class Reducer(kmeans_pb2_grpc.KMeansClusterServicer):
    def shuffle_and_sort(self, intermediate_values):
        sorted_intermediate_values = sorted(intermediate_values, key=lambda x: x[0])
        grouped_values = {}
        '''for key, value in sorted_intermediate_values:
            if key not in grouped_values:
                grouped_values[key] = []
            grouped_values[key].append(value)
        return grouped_values'''
        for i in sorted_intermediate_values:
            x = tuple(i[0])
            if x not in grouped_values:
                grouped_values[x] = []
            grouped_values[x].append(i[1])
        return grouped_values

    def reduce_function(self, centroid_id, grouped_values):
        '''points = grouped_values[centroid_id]
        new_centroid = [sum(x) / len(points) for x in zip(*points)]
        return centroid_id, new_centroid'''
        #find new centroids
        updated = {}
        for key, value in grouped_values.items():
            points = grouped_values[key]
            new_centroid = [sum(x) / len(points) for x in zip(*points)]
            updated[key] = new_centroid
        return updated
        
    def ProcessDataForReducer(self, request, context):
        intermediate_values = []
        '''for i in range(request.num_mappers):
            mapper_dir = f'mapper_{i}'
            with open(os.path.join(mapper_dir, f'partition_{request.reducer_id}.txt'), 'r') as file:
                lines = file.readlines()
                print(lines)
                for line in lines:
                    print(line)
                    centroid, point = line.split(',')
                    print(centroid)
                    print(point)
                    intermediate_values.append((eval(centroid), eval(point)))'''
        
        #print(f"Reducer {request.reducer_id} received intermediate values: {intermediate_values}")

        #perform a grpc call to the mapper to get the intermediate values
        stub = kmeans_pb2_grpc.KMeansClusterStub(grpc.insecure_channel('localhost:5001'))
        response = stub.send_intermediate_values_to_reducer(kmeans_pb2.ReducerRequest(reducer_id=request.reducer_id, num_mappers=request.num_mappers))

        for line in response.data:
            line = line.strip()
            sublists = re.findall(r'\[.*?\]', line)
            centroid = eval(sublists[0])
            point = eval(sublists[1])
            intermediate_values.append((centroid, point))

        grouped_values = self.shuffle_and_sort(intermediate_values)
        new_centroids = self.reduce_function(request.reducer_id, grouped_values)

        output_dir = f'reducer_{request.reducer_id}'
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        output_path = os.path.join(output_dir, 'output.txt')
        with open(output_path, 'w') as f:
            '''for centroid_id, new_centroid in new_centroids:
                f.write(f"{centroid_id} {new_centroid}\n")'''
            for key, value in new_centroids.items():
                f.write(f"{key} {value}\n")

        updated_centroids = []
        for key, value in new_centroids.items():
            updated_centroids.append(value)
        
        request1 = kmeans_pb2.ReducerResponse()
        request1.reducer_id = request.reducer_id
        request1.status = "SUCCESS"
        
        for c in updated_centroids:
            cm = request1.new_centroids.add()
            cm.coordinates.extend(c)

        return request1

        #return kmeans_pb2.ReducerResponse(reducer_id=request.reducer_id, new_centroids=new_centroids, status="SUCCESS")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    kmeans_pb2_grpc.add_KMeansClusterServicer_to_server(Reducer(), server)
    #server.add_insecure_port('[::]:50053')
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    port = sys.argv[1]
    logging.basicConfig(filename='reducer_log.txt', level=logging.INFO)
    print("Reducer server started.")
    serve()
