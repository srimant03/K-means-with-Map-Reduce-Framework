from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MapperRequest(_message.Message):
    __slots__ = ("mapper_id", "centroids", "range_start", "range_end", "num_red")
    MAPPER_ID_FIELD_NUMBER: _ClassVar[int]
    CENTROIDS_FIELD_NUMBER: _ClassVar[int]
    RANGE_START_FIELD_NUMBER: _ClassVar[int]
    RANGE_END_FIELD_NUMBER: _ClassVar[int]
    NUM_RED_FIELD_NUMBER: _ClassVar[int]
    mapper_id: int
    centroids: _containers.RepeatedCompositeFieldContainer[Centroid]
    range_start: int
    range_end: int
    num_red: int
    def __init__(self, mapper_id: _Optional[int] = ..., centroids: _Optional[_Iterable[_Union[Centroid, _Mapping]]] = ..., range_start: _Optional[int] = ..., range_end: _Optional[int] = ..., num_red: _Optional[int] = ...) -> None: ...

class MapperResponse(_message.Message):
    __slots__ = ("mapper_id", "status")
    MAPPER_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    mapper_id: int
    status: str
    def __init__(self, mapper_id: _Optional[int] = ..., status: _Optional[str] = ...) -> None: ...

class ReducerRequest(_message.Message):
    __slots__ = ("reducer_id", "num_mappers")
    REDUCER_ID_FIELD_NUMBER: _ClassVar[int]
    NUM_MAPPERS_FIELD_NUMBER: _ClassVar[int]
    reducer_id: int
    num_mappers: int
    def __init__(self, reducer_id: _Optional[int] = ..., num_mappers: _Optional[int] = ...) -> None: ...

class ReducerResponse(_message.Message):
    __slots__ = ("reducer_id", "new_centroids", "status")
    REDUCER_ID_FIELD_NUMBER: _ClassVar[int]
    NEW_CENTROIDS_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    reducer_id: int
    new_centroids: _containers.RepeatedCompositeFieldContainer[Centroid]
    status: str
    def __init__(self, reducer_id: _Optional[int] = ..., new_centroids: _Optional[_Iterable[_Union[Centroid, _Mapping]]] = ..., status: _Optional[str] = ...) -> None: ...

class IntermediateRequest(_message.Message):
    __slots__ = ("reducer_id", "num_mappers")
    REDUCER_ID_FIELD_NUMBER: _ClassVar[int]
    NUM_MAPPERS_FIELD_NUMBER: _ClassVar[int]
    reducer_id: int
    num_mappers: int
    def __init__(self, reducer_id: _Optional[int] = ..., num_mappers: _Optional[int] = ...) -> None: ...

class IntermediateResponse(_message.Message):
    __slots__ = ("reducer_id", "data")
    REDUCER_ID_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    reducer_id: int
    data: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, reducer_id: _Optional[int] = ..., data: _Optional[_Iterable[str]] = ...) -> None: ...

class Acknowledgement(_message.Message):
    __slots__ = ("message",)
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    message: str
    def __init__(self, message: _Optional[str] = ...) -> None: ...

class Centroid(_message.Message):
    __slots__ = ("coordinates",)
    COORDINATES_FIELD_NUMBER: _ClassVar[int]
    coordinates: _containers.RepeatedScalarFieldContainer[float]
    def __init__(self, coordinates: _Optional[_Iterable[float]] = ...) -> None: ...
