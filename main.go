package main

import (
	"context"
	"fmt"
	"os"

	cid "gx/ipfs/QmcTcsTvfaeEBRFo1TkFgT8sRmgi1n1LTZpecfVP8fzpGD/go-cid"

	fsrepo "github.com/ipfs/go-ipfs/repo/fsrepo"

	blocks "github.com/ipfs/go-ipfs/blocks"
	blockstore "github.com/ipfs/go-ipfs/blocks/blockstore"
	bserv "github.com/ipfs/go-ipfs/blockservice"
	offline "github.com/ipfs/go-ipfs/exchange/offline"
	dag "github.com/ipfs/go-ipfs/merkledag"
	pin "github.com/ipfs/go-ipfs/pin"
)

func fatal(i interface{}) {
	fmt.Println(i)
	os.Exit(1)
}

func convertCid(c *cid.Cid) *cid.Cid {
	if c.Prefix().Codec != 0x72 {
		return c
	}

	return cid.NewCidV1(cid.Raw, c.Hash())
}

func main() {
	fsrp, err := fsrepo.BestKnownPath()
	if err != nil {
		fatal(err)
	}

	r, err := fsrepo.Open(fsrp)
	if err != nil {
		fatal(err)
	}

	bstore := blockstore.NewBlockstore(r.Datastore())

	ctx := context.Background()

	oex := offline.Exchange(bstore)
	ds := dag.NewDAGService(bserv.New(bstore, oex))

	changed := make(map[string]*cid.Cid)

	var processNode func(c *cid.Cid) error
	processNode = func(c *cid.Cid) error {
		_, ok := changed[c.KeyString()]
		if ok {
			return nil
		}

		if c.Prefix().Codec != cid.DagProtobuf {
			return nil
		}

		nd, err := ds.Get(ctx, c)
		if err != nil {
			fmt.Println(err)
			return nil
		}

		pn := nd.(*dag.ProtoNode)
		links := pn.Links()
		var changes bool
		for i := range links {
			thisc := links[i].Cid
			if thisc.Prefix().Codec == 0x72 {
				changes = true
				links[i].Cid = convertCid(thisc)
				continue
			}

			dval, ok := changed[thisc.KeyString()]
			if ok {
				if dval != nil {
					changes = true
					links[i].Cid = dval
					continue
				}
			} else {
				err := processNode(thisc)
				if err != nil {
					return err
				}
			}
		}
		if changes {
			pn.SetLinks(links)
			newc, err := ds.Add(pn)
			if err != nil {
				return err
			}

			fmt.Printf("fixed %s to %s\n", c, newc)
			changed[c.KeyString()] = newc
		} else {
			changed[c.KeyString()] = nil
		}
		return nil
	}

	cids, err := bstore.AllKeysChan(ctx)
	if err != nil {
		fatal(err)
	}
	for c := range cids {
		switch c.Prefix().Codec {
		case 0x72:
			nc := convertCid(c)
			blk, err := bstore.Get(c)
			if err != nil {
				fatal(err)
			}
			nblk, err := blocks.NewBlockWithCid(blk.RawData(), nc)
			if err != nil {
				fatal(err)
			}
			err = bstore.Put(nblk)
			if err != nil {
				fatal(err)
			}

			err = bstore.DeleteBlock(c)
			if err != nil {
				fatal(err)
			}

			changed[c.KeyString()] = nc

		case cid.DagProtobuf:
			err := processNode(c)
			if err != nil {
				fatal(err)
			}
		}
	}

	pinner, err := pin.LoadPinner(r.Datastore(), ds, ds)
	if err != nil {
		fatal(err)
	}

	fmt.Println("============\nFixing potential pinning problems...\n===========")
	for _, c := range pinner.RecursiveKeys() {
		if c.Prefix().Codec == 0x72 {
			pinner.RemovePinWithMode(c, pin.Recursive)
			pinner.PinWithMode(convertCid(c), pin.Recursive)
		}
		dval, ok := changed[c.KeyString()]
		if ok && dval != nil {
			pinner.RemovePinWithMode(c, pin.Recursive)
			pinner.PinWithMode(dval, pin.Recursive)
			fmt.Printf("Changed object %s to %s\n", c, dval)
		}
	}

	for _, c := range pinner.DirectKeys() {
		if c.Prefix().Codec == 0x72 {
			pinner.RemovePinWithMode(c, pin.Direct)
			pinner.PinWithMode(convertCid(c), pin.Direct)
		}
		dval, ok := changed[c.KeyString()]
		if ok && dval != nil {
			pinner.RemovePinWithMode(c, pin.Direct)
			pinner.PinWithMode(dval, pin.Direct)
			fmt.Printf("Changed object %s to %s\n", c, dval)
		}
	}
	if err := pinner.Flush(); err != nil {
		fatal(err)
	}
}
