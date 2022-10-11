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
	reader, _ := server.storage.Reader(req.Context)
	// Reader return's error must be nil
	defer reader.Close()
	resp := new(kvrpcpb.RawGetResponse)
	var err error
	resp.Value, err = reader.GetCF(req.Cf, req.Key)
	resp.NotFound = resp.Value == nil
	return resp, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf}
	err := server.storage.Write(req.Context, []storage.Modify{{Data: put}})
	if err != nil {
		return &kvrpcpb.RawPutResponse{Error: err.Error()}, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	del := storage.Delete{Key: req.Key, Cf: req.Cf}
	err := server.storage.Write(req.Context, []storage.Modify{{Data: del}})
	if err != nil {
		return &kvrpcpb.RawDeleteResponse{Error: err.Error()}, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	r, _ := server.storage.Reader(req.Context)
	//err must be nil
	defer r.Close()
	iter := r.IterCF(req.Cf)
	defer iter.Close()
	resp := new(kvrpcpb.RawScanResponse)
	var i uint32 = 0
	for iter.Seek(req.StartKey); iter.Valid() && i < req.Limit; iter.Next() {
		item := iter.Item()
		pkvpair := new(kvrpcpb.KvPair)
		pkvpair.Key = item.KeyCopy(nil)
		var err error
		pkvpair.Value, err = item.ValueCopy(nil)
		if err != nil {
			return resp, err
		}
		resp.Kvs = append(resp.Kvs, pkvpair)
		i++
	}
	return resp, nil
}
