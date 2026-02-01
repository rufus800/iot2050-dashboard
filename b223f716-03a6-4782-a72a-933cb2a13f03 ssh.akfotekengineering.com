[0;1;32m●[0m cloudflared.service - cloudflared
     Loaded: loaded (]8;;file://iot2050-debian/etc/systemd/system/cloudflared.service/etc/systemd/system/cloudflared.service]8;;; [0;1;32menabled[0m; preset: [0;1;32menabled[0m)
     Active: [0;1;32mactive (running)[0m since Thu 2025-10-30 02:40:12 UTC; 166ms ago
   Main PID: 1733 (cloudflared)
      Tasks: 10 (limit: 2288)
     Memory: 17.4M
        CPU: 667ms
     CGroup: /system.slice/cloudflared.service
             └─[0;38;5;245m1733 /usr/bin/cloudflared --no-autoupdate --config /etc/cloudflared/config.yml tunnel run[0m

Oct 30 02:40:11 iot2050-debian cloudflared[1733]: 2025-10-30T02:40:11Z INF ICMP proxy will use fe80::3360:5ce5:5ca4:21ca in zone eno2 as source for IPv6
Oct 30 02:40:11 iot2050-debian cloudflared[1733]: 2025-10-30T02:40:11Z WRN The user running cloudflared process has a GID (group ID) that is not within ping_group_range. You might need to add that user to a group within that range, or instead update the range to encompass a group the user is already in by modifying /proc/sys/net/ipv4/ping_group_range. Otherwise cloudflared will not be able to ping this network error="Group ID 0 is not between ping group 1 to 0"
Oct 30 02:40:11 iot2050-debian cloudflared[1733]: 2025-10-30T02:40:11Z WRN ICMP proxy feature is disabled error="cannot create ICMPv4 proxy: Group ID 0 is not between ping group 1 to 0 nor ICMPv6 proxy: socket: permission denied"
Oct 30 02:40:11 iot2050-debian cloudflared[1733]: 2025-10-30T02:40:11Z INF ICMP proxy will use 192.168.100.166 as source for IPv4
Oct 30 02:40:11 iot2050-debian cloudflared[1733]: 2025-10-30T02:40:11Z INF ICMP proxy will use fe80::3360:5ce5:5ca4:21ca in zone eno2 as source for IPv6
Oct 30 02:40:11 iot2050-debian cloudflared[1733]: 2025-10-30T02:40:11Z INF Starting metrics server on 127.0.0.1:20241/metrics
Oct 30 02:40:11 iot2050-debian cloudflared[1733]: 2025-10-30T02:40:11Z INF Tunnel connection curve preferences: [X25519MLKEM768 CurveP256] connIndex=0 event=0 ip=198.41.200.193
Oct 30 02:40:12 iot2050-debian cloudflared[1733]: 2025-10-30T02:40:12Z INF Registered tunnel connection connIndex=0 connection=5aabcfe7-2479-4f60-a4bb-3c8e9062ec41 event=0 ip=198.41.200.193 location=cdg07 protocol=quic
Oct 30 02:40:12 iot2050-debian cloudflared[1733]: 2025-10-30T02:40:12Z INF Tunnel connection curve preferences: [X25519MLKEM768 CurveP256] connIndex=1 event=0 ip=198.41.192.167
Oct 30 02:40:12 iot2050-debian systemd[1]: Started cloudflared.service - cloudflared.
