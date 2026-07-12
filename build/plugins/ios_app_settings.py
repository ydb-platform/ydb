from ymake import macro, Unit


@macro
def IOS_APP_SETTINGS(unit: Unit, *, OS_VERSION: str = '', DEVICES: tuple[str, ...] = ()):
    if OS_VERSION:
        unit.onios_app_common_flags(['--minimum-deployment-target', OS_VERSION])
        unit.onios_app_assets_flags(['--filter-for-device-os-version', OS_VERSION])
    devices_flags = []
    for device in DEVICES:
        devices_flags += ['--target-device', device]
    if devices_flags:
        unit.onios_app_common_flags(devices_flags)
