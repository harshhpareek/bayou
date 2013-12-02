package ut.distcomp.framework;

import java.util.ArrayList;

public class Client extends Process {
	int client_id;
	int wid=0;

	public Client(Env env, ProcessId me, int client_id){
		this.env=env;
		this.me=me;
		this.client_id= client_id;
		env.addProc(me, this);
	}

	@Override
	void body() {
		System.out.println("Here I am: " + me);

		while(true){
			BayouMessage msg = getNextMessage();
			if(msg instanceof ClientUpdateMessage){				
				ClientUpdateMessage m = (ClientUpdateMessage) msg;
				sendMessage(m.dst, new WIDQuery(me, client_id));
				ArrayList<BayouMessage> pendingMessages = new ArrayList<BayouMessage>();
				while(true){
					BayouMessage msg1 = getNextMessage();
					if(msg1 instanceof WIDMsg){
						WIDMsg m1 = (WIDMsg) msg1;
						if((m1.WID==0 && wid==0) || m1.WID==wid){
							wid++;
							sendMessage(m.dst, new UpdateMessage(me, m.updateStr,wid, client_id));
							break;
						} else{
							System.err.println("Server not up to date. nodeID is: "+m1.src+" and Server_wid="+m1.WID+" My wid="+wid+" at clientID: "+client_id+" and command is: "+m.updateStr);
							//TODO maybe do roundrobin
							break;
						}
					}
					else{
						pendingMessages.add(msg1);
					}
				}
				for(BayouMessage m2:pendingMessages){
					deliver(m2);
				}
			}
			else if(msg instanceof ClientQueryMessage){
				ClientQueryMessage m = (ClientQueryMessage) msg;
				sendMessage(m.dst, new WIDQuery(me, client_id));
				ArrayList<BayouMessage> pendingMessages = new ArrayList<BayouMessage>();
				while(true){
					BayouMessage msg1 = getNextMessage();
					if(msg1 instanceof WIDMsg){
						WIDMsg m1 = (WIDMsg) msg1;
						if(m1.WID==wid){
							sendMessage(m.dst, new QueryMessage(me, m.songName,wid, client_id));
							break;
						} else{
							//TODO maybe do roundrobin
							System.err.println("Server not up to date. nodeID is: "+m1.src+" and Server_wid="+m1.WID+" My wid="+wid+" at clientID: "+client_id+" and command is: "+m.songName);
							break;
						}
					}
					else{
						pendingMessages.add(msg1);
					}
				}
				for(BayouMessage m2:pendingMessages){
					deliver(m2);
				}
			}
			else if(msg instanceof ResponseMessage){
				ResponseMessage m = (ResponseMessage) msg;
				System.err.println("Received response "+m.response+" from "+m.src+" at client: "+client_id);
			}
		}
	}

	ProcessId myConnectedNode(){
		if(env.clientConnections[client_id]==-1)
			return null;
		else
			return env.Nodes.getProcessId(env.clientConnections[client_id]);
	}
}
