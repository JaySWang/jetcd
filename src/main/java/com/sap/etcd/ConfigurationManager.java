package com.sap.etcd;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import com.google.common.util.concurrent.ListenableFuture;
import com.justinsb.etcd.EtcdClient;
import com.justinsb.etcd.EtcdClientException;
import com.justinsb.etcd.EtcdNode;
import com.justinsb.etcd.EtcdResult;

public class ConfigurationManager {
	private String prefix = "/conf";
	private String etcdServerURL = "http://127.0.0.1:4001/";
	private  EtcdClient client;
	private Map<String,String> confKeyPairs = new ConcurrentHashMap<String,String>();
	ListenableFuture<EtcdResult> consistentWatchFuture; 	

	public void initialize() throws EtcdClientException {
		this.client = new EtcdClient(URI.create(etcdServerURL));
		consistentWatch(prefix);
	}

	private void consistentWatch(String key) throws EtcdClientException{
		
		EtcdResult result=client.get(key);
		if(result==null){
			result = client.set(key, "null");			
		}
		initializeConfs(result);
		consistentWatchFuture = client.watch(key,null,true);
		consistentWatchFuture.addListener(new Runnable(){
			@Override
			public void run() {
				try {
					EtcdResult watchResult =consistentWatchFuture.get();
					confKeyPairs.put(watchResult.node.key,watchResult.node.value);
					System.out.println(confKeyPairs.get(watchResult.node.key));
					consistentWatchFuture = client.watch(key,
						watchResult.node.modifiedIndex + 1,
						true);
					consistentWatchFuture.addListener(this , Executors.newFixedThreadPool(4));
				} catch (Exception e) {
					e.printStackTrace();
				} 
			}
			},
			Executors.newFixedThreadPool(4));	
	}
	
	private void initializeConfs(EtcdResult result) {
		List<EtcdNode> nodes = result.node.nodes;
		for(EtcdNode eNode:nodes){
			confKeyPairs.put(eNode.key,eNode.value);
		}
	}

	public static void main(String[] args){
		ConfigurationManager confManager = new ConfigurationManager();
		try {
			confManager.initialize();
		} catch (EtcdClientException e) {
			e.printStackTrace();
		}
		// curl -L http://127.0.0.1:4001/v2/keys/conf/someUrl1 -XPUT -d value=10.10.1
	}
	
	
	
	

}
