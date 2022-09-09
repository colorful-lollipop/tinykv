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
	r, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	resp := new(kvrpcpb.RawGetResponse)
	val, err := r.GetCF(req.Cf, req.Key)
	if err != nil {
		return nil, err
	}
	if val == nil {
		resp.NotFound = true
	}
	resp.Value = val
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	put := storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf}
	err := server.storage.Write(req.Context, []storage.Modify{{Data: put}})
	if err != nil {
		return nil, err
	}
	resp := new(kvrpcpb.RawPutResponse)
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	del := storage.Delete{Key: req.Key, Cf: req.Cf}
	err := server.storage.Write(req.Context, []storage.Modify{{Data: del}})
	if err != nil {
		return nil, err
	}
	resp := new(kvrpcpb.RawDeleteResponse)
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	r, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	iter := r.IterCF(req.Cf)
	defer iter.Close()
	resp := new(kvrpcpb.RawScanResponse)
	var i uint32
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		kvp := new(kvrpcpb.KvPair)
		kvp.Key = item.KeyCopy(nil)
		kvp.Value, err = item.ValueCopy(nil)
		if err != nil {
			return resp, err
		}
		resp.Kvs = append(resp.Kvs, kvp)
		i++
		if i == req.Limit {
			break
		}
	}
	return resp, nil
}
