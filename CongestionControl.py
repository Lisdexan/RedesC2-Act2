class CongestionControl:
    SLOW_START = "slow_start"
    CONGESTION_AVOIDANCE = "congestion_avoidance"

    def __init__(self, MSS:int):
        self.current_state = CongestionControl.SLOW_START
        self.MSS = MSS
        self.cwnd = MSS                         
        self.ssthresh = None  

    def get_cwnd(self):
        return self.cwnd
    
    def get_MSS_in_cwnd(self):
        calcMSS= int(self.cwnd // self.MSS)
        return calcMSS

    
    def event_ack_received(self):
        if self.current_state==CongestionControl.SLOW_START:
            #+1 MSS por ACK
            self.cwnd += self.MSS
            #si pasamos el umbral, seguimos en congestion avoidance
            if self.ssthresh is not None and self.cwnd >= self.ssthresh:
                self.current_state = CongestionControl.CONGESTION_AVOIDANCE
        else:
            #+(1/MSS_en_cwnd) de MSS por ACK,
            self.cwnd += (1 / self.get_MSS_in_cwnd()) * self.MSS

    def event_timeout(self):
        self.ssthresh=int(self.cwnd/2)
        self.cwnd=self.MSS
        self.current_state=CongestionControl.SLOW_START


    def get_ssthresh(self):
        return self.ssthresh
 
    def is_state_slow_start(self) -> bool:
        return self.current_state == CongestionControl.SLOW_START
 
    def is_state_congestion_avoidance(self) -> bool:
        return self.current_state == CongestionControl.CONGESTION_AVOIDANCE
 