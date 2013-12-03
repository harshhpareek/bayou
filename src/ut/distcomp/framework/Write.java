package ut.distcomp.framework;

public class Write implements Comparable<Write>{
	String serverID;
	int accept_stamp;
	int CSN=-1;
	String command;
	int client_id;
	int wid=-1;
	boolean nonRejectable=false;

	public Write(String id, int accept_stamp, int csn, String command, int wid, int client_id){
		this.serverID=id;
		this.accept_stamp=accept_stamp;
		this.CSN=csn;
		this.command=command;
		this.wid = wid;
		this.client_id = client_id;
	}
	public Write(String id, int accept_stamp, int csn, String command, int wid, int client_id,boolean nonR){
		this.serverID=id;
		this.accept_stamp=accept_stamp;
		this.CSN=csn;
		this.command=command;
		this.wid = wid;
		this.client_id = client_id;
		this.nonRejectable=nonR;
	}
	public int compareTo(Write c) {
		if(this.CSN!=-1 && c.CSN!=-1)
			return this.CSN - c.CSN;
		else if(this.accept_stamp!=c.accept_stamp)
			return this.accept_stamp-c.accept_stamp;
		else
			return this.serverID.compareTo(c.serverID); //TODO check this
	}
	
	public String toString(){
		return "(serverID="+serverID+",ac_stamp="+accept_stamp+",CSN="+CSN+",client_id="+client_id+",wid="+wid+",command="+command+")";
	}
}
