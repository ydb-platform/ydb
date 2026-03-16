import os
from subprocess import Popen, PIPE
import datetime
import dateutil.parser

import cv2

from opensfm import context
from opensfm import geotag_from_gpx
from opensfm import io


def video_orientation(video_file):
    # Rotation
    rotation = Popen(['exiftool', '-Rotation', '-b', video_file], stdout=PIPE).stdout.read()
    if rotation:
        rotation = float(rotation)
        if rotation == 0:
            orientation = 1
        elif rotation == 90:
            orientation = 6
        elif rotation == 180:
            orientation = 3
        elif rotation == 270:
            orientation = 8
    else:
        orientation = 1
    return orientation


def import_video_with_gpx(video_file, gpx_file, output_path, dx, dt=None, start_time=None, visual=False, image_description=None):

    points = geotag_from_gpx.get_lat_lon_time(gpx_file)

    orientation = video_orientation(video_file)

    if start_time:
        video_start_time = dateutil.parser.parse(start_time)
    else:
        try:
            exifdate = Popen(['exiftool', '-CreateDate', '-b', video_file], stdout=PIPE).stdout.read()
            video_start_time = datetime.datetime.strptime(exifdate,'%Y:%m:%d %H:%M:%S')
        except:
            print('Video recording timestamp not found. Using first GPS point time.')
            video_start_time = points[0][0]
        try:
            duration = Popen(['exiftool', '-MediaDuration', '-b', video_file], stdout=PIPE).stdout.read()
            video_duration = float(duration)
            video_end_time = video_start_time + datetime.timedelta(seconds=video_duration)
        except:
            print('Video end time not found. Using last GPS point time.')
            video_end_time = points[-1][0]

    print('GPS track starts at: {}'.format(points[0][0]))
    print('Video starts at: {}'.format(video_start_time))

    # Extract video frames.
    io.mkdir_p(output_path)
    key_points = geotag_from_gpx.sample_gpx(points, dx, dt)

    cap = cv2.VideoCapture(video_file)
    image_files = []
    for p in key_points:
        dt = (p[0] - video_start_time).total_seconds()
        if dt > 0:
            CAP_PROP_POS_MSEC = cv2.CAP_PROP_POS_MSEC if context.OPENCV3 else cv2.cv.CV_CAP_PROP_POS_MSEC
            cap.set(CAP_PROP_POS_MSEC, int(dt * 1000))
            ret, frame = cap.read()
            if ret:
                print('Grabbing frame for time {}'.format(p[0]))
                filepath = os.path.join(output_path, p[0].strftime("%Y_%m_%d_%H_%M_%S_%f")[:-3] + '.jpg')
                cv2.imwrite(filepath, frame)
                geotag_from_gpx.add_exif_using_timestamp(filepath, points, timestamp=p[0], orientation=orientation)

                # Display the resulting frame
                if visual:
                    # Display the resulting frame
                    max_display_size = 800
                    resize_ratio = float(max_display_size) / max(frame.shape[0], frame.shape[1])
                    frame = cv2.resize(frame, dsize=(0, 0), fx=resize_ratio, fy=resize_ratio)
                    cv2.imshow('frame', frame)
                    if cv2.waitKey(1) & 0xFF == 27:
                        break
                image_files.append(filepath)
    # When everything done, release the capture
    cap.release()
    if visual:
        cv2.destroyAllWindows()
    return image_files
