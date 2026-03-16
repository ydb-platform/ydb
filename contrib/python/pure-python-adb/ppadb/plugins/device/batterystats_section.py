
class Version:
    def __init__(self, checkin_version,parcel_version,start_platform_version,end_platform_version):
        self.id = "vers"
        self.checkin_version = checkin_version
        self.parcel_version = parcel_version
        self.start_platform_version = start_platform_version
        self.end_platform_version = end_platform_version


class UID:
    def __init__(self, uid,package_name):
        self.id = "uid"
        self.uid = uid
        self.package_name = package_name


class APK:
    def __init__(self, wakeups,apk,service,start_time,starts,launches):
        self.id = "apk"
        self.wakeups = wakeups
        self.apk = apk
        self.service = service
        self.start_time = start_time
        self.starts = starts
        self.launches = launches


class Process:
    def __init__(self, process,user,system,foreground,starts):
        self.id = "pr"
        self.process = process
        self.user = user
        self.system = system
        self.foreground = foreground
        self.starts = starts


class Sensor:
    def __init__(self, sensor_number,time,count):
        self.id = "sr"
        self.sensor_number = sensor_number
        self.time = time
        self.count = count


class Vibrator:
    def __init__(self, time,count):
        self.id = "vib"
        self.time = time
        self.count = count


class Foreground:
    def __init__(self, time,count):
        self.id = "fg"
        self.time = time
        self.count = count


class StateTime:
    def __init__(self, foreground,active,running):
        self.id = "st"
        self.foreground = foreground
        self.active = active
        self.running = running


class Wakelock:
    def __init__(self, wake_lock,full_time,f,full_count,partial_time,p,partial_count,window_time,w,window_count):
        self.id = "wl"
        self.wake_lock = wake_lock
        self.full_time = full_time
        self.f = f
        self.full_count = full_count
        self.partial_time = partial_time
        self.p = p
        self.partial_count = partial_count
        self.window_time = window_time
        self.w = w
        self.window_count = window_count


class Sync:
    def __init__(self, sync,time,count):
        self.id = "sy"
        self.sync = sync
        self.time = time
        self.count = count


class Job:
    def __init__(self, job,time,count):
        self.id = "jb"
        self.job = job
        self.time = time
        self.count = count


class KernelWakeLock:
    def __init__(self, kernel_wake_lock,time,count):
        self.id = "kwl"
        self.kernel_wake_lock = kernel_wake_lock
        self.time = time
        self.count = count


class WakeupReason:
    def __init__(self, wakeup_reason,time,count):
        self.id = "wr"
        self.wakeup_reason = wakeup_reason
        self.time = time
        self.count = count


class Network:
    def __init__(self, mobile_bytes_rx,mobile_bytes_tx,wifi_bytes_rx,wifi_bytes_tx,mobile_packets_rx,mobile_packets_tx,wifi_packets_rx,wifi_packets_tx,mobile_active_time,mobile_active_count):
        self.id = "nt"
        self.mobile_bytes_rx = mobile_bytes_rx
        self.mobile_bytes_tx = mobile_bytes_tx
        self.wifi_bytes_rx = wifi_bytes_rx
        self.wifi_bytes_tx = wifi_bytes_tx
        self.mobile_packets_rx = mobile_packets_rx
        self.mobile_packets_tx = mobile_packets_tx
        self.wifi_packets_rx = wifi_packets_rx
        self.wifi_packets_tx = wifi_packets_tx
        self.mobile_active_time = mobile_active_time
        self.mobile_active_count = mobile_active_count


class UserActivity:
    def __init__(self, other,button,touch):
        self.id = "ua"
        self.other = other
        self.button = button
        self.touch = touch


class Battery:
    def __init__(self, start_count,battery_realtime,battery_uptime,total_realtime,total_uptime,start_clock_time,battery_screen_off_realtime,battery_screen_off_uptime):
        self.id = "bt"
        self.start_count = start_count
        self.battery_realtime = battery_realtime
        self.battery_uptime = battery_uptime
        self.total_realtime = total_realtime
        self.total_uptime = total_uptime
        self.start_clock_time = start_clock_time
        self.battery_screen_off_realtime = battery_screen_off_realtime
        self.battery_screen_off_uptime = battery_screen_off_uptime


class BatteryDischarge:
    def __init__(self, low,high,screen_on,screen_off):
        self.id = "dc"
        self.low = low
        self.high = high
        self.screen_on = screen_on
        self.screen_off = screen_off


class BatteryLevel:
    def __init__(self, start_level,current_level):
        self.id = "lv"
        self.start_level = start_level
        self.current_level = current_level


class WiFi:
    def __init__(self, full_wifi_lock_on_time,wifi_scan_time,wifi_running_time,wifi_scan_count,wifi_idle_time,wifi_receive_time,wifi_transmit_time):
        self.id = "wfl"
        self.full_wifi_lock_on_time = full_wifi_lock_on_time
        self.wifi_scan_time = wifi_scan_time
        self.wifi_running_time = wifi_running_time
        self.wifi_scan_count = wifi_scan_count
        self.wifi_idle_time = wifi_idle_time
        self.wifi_receive_time = wifi_receive_time
        self.wifi_transmit_time = wifi_transmit_time


class GlobalWiFi:
    def __init__(self, wifi_on_time,wifi_running_time,wifi_idle_time,wifi_receive_time,wifi_transmit_time,wifi_power_mah):
        self.id = "gwfl"
        self.wifi_on_time = wifi_on_time
        self.wifi_running_time = wifi_running_time
        self.wifi_idle_time = wifi_idle_time
        self.wifi_receive_time = wifi_receive_time
        self.wifi_transmit_time = wifi_transmit_time
        self.wifi_power_mah = wifi_power_mah


class GlobalBluetooth:
    def __init__(self, bt_idle_time,bt_receive_time,bt_transmit_time,bt_power_mah):
        self.id = "gble"
        self.bt_idle_time = bt_idle_time
        self.bt_receive_time = bt_receive_time
        self.bt_transmit_time = bt_transmit_time
        self.bt_power_mah = bt_power_mah


class Misc:
    def __init__(self, screen_on_time,phone_on_time,full_wakelock_time_total,partial_wakelock_time_total,mobile_radio_active_time,mobile_radio_active_adjusted_time,interactive_time,power_save_mode_enabled_time,connectivity_changes,device_idle_mode_enabled_time,device_idle_mode_enabled_count,device_idling_time,device_idling_count,mobile_radio_active_count,mobile_radio_active_unknown_time):
        self.id = "m"
        self.screen_on_time = screen_on_time
        self.phone_on_time = phone_on_time
        self.full_wakelock_time_total = full_wakelock_time_total
        self.partial_wakelock_time_total = partial_wakelock_time_total
        self.mobile_radio_active_time = mobile_radio_active_time
        self.mobile_radio_active_adjusted_time = mobile_radio_active_adjusted_time
        self.interactive_time = interactive_time
        self.power_save_mode_enabled_time = power_save_mode_enabled_time
        self.connectivity_changes = connectivity_changes
        self.device_idle_mode_enabled_time = device_idle_mode_enabled_time
        self.device_idle_mode_enabled_count = device_idle_mode_enabled_count
        self.device_idling_time = device_idling_time
        self.device_idling_count = device_idling_count
        self.mobile_radio_active_count = mobile_radio_active_count
        self.mobile_radio_active_unknown_time = mobile_radio_active_unknown_time


class GlobalNetwork:
    def __init__(self, mobile_rx_total_bytes,mobile_tx_total_bytes,wifi_rx_total_bytes,wifi_tx_total_bytes,mobile_rx_total_packets,mobile_tx_total_packets,wifi_rx_total_packets,wifi_tx_total_packets):
        self.id = "gn"
        self.mobile_rx_total_bytes = mobile_rx_total_bytes
        self.mobile_tx_total_bytes = mobile_tx_total_bytes
        self.wifi_rx_total_bytes = wifi_rx_total_bytes
        self.wifi_tx_total_bytes = wifi_tx_total_bytes
        self.mobile_rx_total_packets = mobile_rx_total_packets
        self.mobile_tx_total_packets = mobile_tx_total_packets
        self.wifi_rx_total_packets = wifi_rx_total_packets
        self.wifi_tx_total_packets = wifi_tx_total_packets


class ScreenBrightness:
    def __init__(self, dark,dim,medium,light,bright):
        self.id = "br"
        self.dark = dark
        self.dim = dim
        self.medium = medium
        self.light = light
        self.bright = bright


class SignalScanningTime:
    def __init__(self, signal_scanning_time):
        self.id = "sst"
        self.signal_scanning_time = signal_scanning_time


class SignalStrengthTime:
    def __init__(self, none,poor,moderate,good,great):
        self.id = "sgt"
        self.none = none
        self.poor = poor
        self.moderate = moderate
        self.good = good
        self.great = great


class SignalStrengthCount:
    def __init__(self, none,poor,moderate,good,great):
        self.id = "sgc"
        self.none = none
        self.poor = poor
        self.moderate = moderate
        self.good = good
        self.great = great


class DataConnectionTime:
    def __init__(self, none,gprs,edge,umts,cdma,evdo_0,evdo_a,_1xrtt,hsdpa,hsupa,hspa,iden,evdo_b,lte,ehrpd,hspap,other):
        self.id = "dct"
        self.none = none
        self.gprs = gprs
        self.edge = edge
        self.umts = umts
        self.cdma = cdma
        self.evdo_0 = evdo_0
        self.evdo_a = evdo_a
        self._1xrtt = _1xrtt
        self.hsdpa = hsdpa
        self.hsupa = hsupa
        self.hspa = hspa
        self.iden = iden
        self.evdo_b = evdo_b
        self.lte = lte
        self.ehrpd = ehrpd
        self.hspap = hspap
        self.other = other


class DataConnectionCount:
    def __init__(self, none,gprs,edge,umts,cdma,evdo_0,evdo_a,_1xrtt,hsdpa,hsupa,hspa,iden,evdo_b,lte,ehrpd,hspap,other):
        self.id = "dcc"
        self.none = none
        self.gprs = gprs
        self.edge = edge
        self.umts = umts
        self.cdma = cdma
        self.evdo_0 = evdo_0
        self.evdo_a = evdo_a
        self._1xrtt = _1xrtt
        self.hsdpa = hsdpa
        self.hsupa = hsupa
        self.hspa = hspa
        self.iden = iden
        self.evdo_b = evdo_b
        self.lte = lte
        self.ehrpd = ehrpd
        self.hspap = hspap
        self.other = other


class WiFiStateTime:
    def __init__(self, off,off_scanning,on_no_networks,on_disconnected,on_connected_sta,on_connected_p2p,on_connected_sta_p2p,soft_ap):
        self.id = "wst"
        self.off = off
        self.off_scanning = off_scanning
        self.on_no_networks = on_no_networks
        self.on_disconnected = on_disconnected
        self.on_connected_sta = on_connected_sta
        self.on_connected_p2p = on_connected_p2p
        self.on_connected_sta_p2p = on_connected_sta_p2p
        self.soft_ap = soft_ap


class WiFiStateCount:
    def __init__(self, off,off_scanning,on_no_networks,on_disconnected,on_connected_sta,on_connected_p2p,on_connected_sta_p2p,soft_ap):
        self.id = "wsc"
        self.off = off
        self.off_scanning = off_scanning
        self.on_no_networks = on_no_networks
        self.on_disconnected = on_disconnected
        self.on_connected_sta = on_connected_sta
        self.on_connected_p2p = on_connected_p2p
        self.on_connected_sta_p2p = on_connected_sta_p2p
        self.soft_ap = soft_ap


class WiFiSupplicantStateTime:
    def __init__(self, invalid,disconnected,interface_disabled,inactive,scanning,authenticating,associating,associated,four_way_handshake,group_handshake,completed,dormant,uninitialized):
        self.id = "wsst"
        self.invalid = invalid
        self.disconnected = disconnected
        self.interface_disabled = interface_disabled
        self.inactive = inactive
        self.scanning = scanning
        self.authenticating = authenticating
        self.associating = associating
        self.associated = associated
        self.four_way_handshake = four_way_handshake
        self.group_handshake = group_handshake
        self.completed = completed
        self.dormant = dormant
        self.uninitialized = uninitialized


class WiFiSupplicantStateCount:
    def __init__(self, invalid,disconnected,interface_disabled,inactive,scanning,authenticating,associating,associated,four_way_handshake,group_handshake,completed,dormant,uninitialized):
        self.id = "wssc"
        self.invalid = invalid
        self.disconnected = disconnected
        self.interface_disabled = interface_disabled
        self.inactive = inactive
        self.scanning = scanning
        self.authenticating = authenticating
        self.associating = associating
        self.associated = associated
        self.four_way_handshake = four_way_handshake
        self.group_handshake = group_handshake
        self.completed = completed
        self.dormant = dormant
        self.uninitialized = uninitialized


class WiFiSignalStrengthTime:
    def __init__(self, none,poor,moderate,good,great):
        self.id = "wsgt"
        self.none = none
        self.poor = poor
        self.moderate = moderate
        self.good = good
        self.great = great


class WiFiSignalStrengthCount:
    def __init__(self, none,poor,moderate,good,great):
        self.id = "wsgc"
        self.none = none
        self.poor = poor
        self.moderate = moderate
        self.good = good
        self.great = great


class BluetoothStateTime:
    def __init__(self, inactive,low,med,high):
        self.id = "bst"
        self.inactive = inactive
        self.low = low
        self.med = med
        self.high = high


class BluetoothStateCount:
    def __init__(self, inactive,low,med,high):
        self.id = "bsc"
        self.inactive = inactive
        self.low = low
        self.med = med
        self.high = high

mapping={
'vers':Version,
'uid':UID,
'apk':APK,
'pr':Process,
'sr':Sensor,
'vib':Vibrator,
'fg':Foreground,
'st':StateTime,
'wl':Wakelock,
'sy':Sync,
'jb':Job,
'kwl':KernelWakeLock,
'wr':WakeupReason,
'nt':Network,
'ua':UserActivity,
'bt':Battery,
'dc':BatteryDischarge,
'lv':BatteryLevel,
'wfl':WiFi,
'gwfl':GlobalWiFi,
'gble':GlobalBluetooth,
'm':Misc,
'gn':GlobalNetwork,
'br':ScreenBrightness,
'sst':SignalScanningTime,
'sgt':SignalStrengthTime,
'sgc':SignalStrengthCount,
'dct':DataConnectionTime,
'dcc':DataConnectionCount,
'wst':WiFiStateTime,
'wsc':WiFiStateCount,
'wsst':WiFiSupplicantStateTime,
'wssc':WiFiSupplicantStateCount,
'wsgt':WiFiSignalStrengthTime,
'wsgc':WiFiSignalStrengthCount,
'bst':BluetoothStateTime,
'bsc':BluetoothStateCount
}

def get_section(name):
    return mapping.get(name)
