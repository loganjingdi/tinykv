package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).

	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, nil
	}

	data, err := reader.GetCF(req.Cf, req.Key)

	response := kvrpcpb.RawGetResponse{
		Value: data,
		NotFound: false,
	}

	if data == nil {
		response.NotFound = true
	}

	reader.Close()

	return &response, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	modify := storage.Modify {
		Data: storage.Put {
			Cf:    req.Cf,
			Key:   req.Key,
			Value: req.Value,
		},
	}

	err := server.storage.Write(nil, []storage.Modify{modify})

	response := kvrpcpb.RawPutResponse{}

	if err != nil {
		response.Error = err.Error()
		return &response , err
	}

	return &response, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	del := storage.Modify {
		Data: storage.Delete {
			Cf:    req.Cf,
			Key:   req.Key,
		},
	}

	err := server.storage.Write(nil, []storage.Modify{del})

	response := kvrpcpb.RawDeleteResponse{}

	if err != nil {
		response.Error = err.Error()
		return &response , err
	}

	return &response, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	var kvPairs []*kvrpcpb.KvPair

	iter := reader.IterCF(req.Cf)
	iter.Seek(req.StartKey)
	limit := req.Limit

	for ; iter.Valid(); iter.Next() {
		if limit <= 0 {
			break
		}
		key := iter.Item().Key()
		val, _ := iter.Item().Value()
		kvPairs = append(kvPairs, &kvrpcpb.KvPair {
			Key: key,
			Value: val,
		})
		limit--
	}

	response := kvrpcpb.RawScanResponse{
		Kvs: kvPairs,
	}

	return &response, nil
}
