# Lab 11.1 Stream Data Pipeline II - Overview and Producer¶

- Scenario: Streaming audio 

Stream in audio, process, calling a machine learning classification model and save the data for reporting.

Create a new jupyter notebook file "stream\_data\_pipeline\_2\_producer.ipynb".

In [ ]:

```
import os
os.chdir('/Users/<username>/projects/ee3801')
```

# 1. Scenario: Streaming audio¶

The company would like to build an in-house automatic speech transcribing tool. We record the streams of audio from one computer and another computer can receive this audio and transcribe in real-time. The system stream in audio, transcribe it using Open AI's whisper model and transcribed text is saved for reporting in real-time.

# 1.1 Single stream audio data auto-transcription¶

In the previous lab exercise, you have observed missing words lost in recording and transcription. In this lab, you will attempt to capture all audio streams and transcribe to text. Take note of the time taken to read, write and transcribe the audio.

You will need two notebook to run concurrently.

- Producer - codes below
- Consumer - stream\_data\_pipeline\_2\_consumer.ipynb

In [ ]:

```
# # install python packages
# !pip install --upgrade pip
# # for mac users
# !brew install portaudio 
# !pip3 install pyaudio
```

# 1.2 Stream in audio¶

In [ ]:

```
# Testing audio setup in this device
import pyaudio

audio = pyaudio.PyAudio()
print("audio.get_device_count():",audio.get_device_count())
for i in range(audio.get_device_count()):
    print(audio.get_device_info_by_index(i))
```

In [ ]:

```
# This is to determine which input audio and output audio you will use.
# Explore and find the right index to use for input and output in your device.
print("Choosing my input audio:",audio.get_device_info_by_index(2))
print("Choosing my output audio:",audio.get_device_info_by_index(3))
audio.terminate()
```

# 2. Load whisper model once¶

In [ ]:

```
# !pip3 install -U jupyter
# !pip3 install -U ipywidgets
# !pip3 uninstall whisper -y
# !pip3 install openai-whisper
# !pip3 install -U numpy
# !pip3 install -U openai-whisper
```

In [ ]:

```
## python 3.11.5
# !pip3 install -U torch
# !pip3 uninstall numpy -y
# !pip3 install numpy==1.26.4
## restart kernel
```

# 3. Read from the script as you are recording¶

```
Producers are fairly straightforward – they send messages to a topic and partition, maybe request an acknowledgment, retry if a message fails – or not – and continue. Consumers, however, can be a little more complicated.

Consumers read messages from a topic. Consumers run in a poll loop that runs indefinitely waiting for messages. Consumers can read from the beginning – they will start at the first message in the topic and read the entire history. Once caught up, the consumer will wait for new messages.
```

# 4. Capture every sentence in a paragraph¶

Using the device's audio capture the audio, record the sentence, send the audio data using Producer to a topic.

In [ ]:

```
# if timeout add 127.0.0.1 host.docker.internal to /etc/hosts
```

# 4.1 Initialise Producer¶

In [ ]:

```
# kafka-python Producer
from kafka import KafkaProducer
from kafka.errors import KafkaError

# produce asynchronously with callbacks
producer = KafkaProducer(bootstrap_servers=['localhost:29092','localhost:39092','localhost:49092']) 

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    print('I am an errback', exc_info=excp)
    # handle exception
```

# 4.2 Single thread audio Producer¶

In [ ]:

```
# Single thread audio

import pyaudio
import wave
import numpy as np
from datetime import datetime
import whisper
import sys

FORMAT = pyaudio.paInt16
CHUNK = 1024
RECORD_SECONDS = 5
DEVICE_ID = 2

audio = pyaudio.PyAudio()
RATE = int(audio.get_device_info_by_index(DEVICE_ID)['defaultSampleRate'])
CHANNELS = int(audio.get_device_info_by_index(DEVICE_ID)['maxInputChannels'])

audio = pyaudio.PyAudio()
stream = audio.open(
    format=FORMAT,
    channels=CHANNELS,
    rate=RATE,
    input=True,
    frames_per_buffer=CHUNK,
    input_device_index=DEVICE_ID
)

start_time = datetime.now()

try:
    while True:
        before_time = datetime.now()
        frames = []
        for i in range(0, int(RATE / CHUNK * RECORD_SECONDS)):
            data = stream.read(CHUNK, exception_on_overflow = False)
            frames.append(data)
        raw_data = b''.join(frames)

        # produce asynchronously with callbacks
        producer.send('dataengineering', raw_data, key="audio".encode('utf-8'))\
                .add_callback(on_send_success)\
                .add_errback(on_send_error)
        print("%s audio_duration (s): %s" % (datetime.now().strftime("%d/%m/%Y, %H:%M:%S"), (datetime.now() - before_time).seconds))

        # block until all async messages are sent
        producer.flush()

        if (datetime.now() - start_time).seconds > 60: #exit program after 1min
            stream.stop_stream()
            stream.close()
            audio.terminate()
            print("* Exit program after 1min *")
            break
        
except KeyboardInterrupt as kie:
    print("* Program terminated by user *")
    stream.stop_stream()
    stream.close()
    audio.terminate()
except Exception as e:
    # print("Exception:", e)
    if stream!=None:
        stream.stop_stream()
        stream.close()
        audio.terminate()
sys.exit(0)
```

1. Start the Consumer (stream\_data\_pipeline\_2\_consumer.ipynb) to receive text from this producer.
2. What did you observe from the messages sent? Submit your findings.

# 4.3 Multiple thread audio Producer (Optional)¶

In [ ]:

```
# Multiple thread audio

import pyaudio
import wave
import numpy as np
from datetime import datetime
import whisper
import sys

FORMAT = pyaudio.paInt16
CHUNK = 1024
RECORD_SECONDS = 5
DEVICE_ID_1 = 1
DEVICE_ID_2 = 2
DEVICE_ID_3 = 4

audio = pyaudio.PyAudio()
RATE = int(audio.get_device_info_by_index(DEVICE_ID_1)['defaultSampleRate'])
CHANNELS = int(audio.get_device_info_by_index(DEVICE_ID_1)['maxInputChannels'])

stream1 = audio.open(
    format=FORMAT,
    channels=CHANNELS,
    rate=RATE,
    input=True,
    frames_per_buffer=CHUNK,
    input_device_index=DEVICE_ID_1
)

audio = pyaudio.PyAudio()
RATE = int(audio.get_device_info_by_index(DEVICE_ID_2)['defaultSampleRate'])
CHANNELS = int(audio.get_device_info_by_index(DEVICE_ID_2)['maxInputChannels'])

stream2 = audio.open(
    format=FORMAT,
    channels=CHANNELS,
    rate=RATE,
    input=True,
    frames_per_buffer=CHUNK,
    input_device_index=DEVICE_ID_2
)

audio = pyaudio.PyAudio()
RATE = int(audio.get_device_info_by_index(DEVICE_ID_3)['defaultSampleRate'])
CHANNELS = int(audio.get_device_info_by_index(DEVICE_ID_3)['maxInputChannels'])

stream3 = audio.open(
    format=FORMAT,
    channels=CHANNELS,
    rate=RATE,
    input=True,
    frames_per_buffer=CHUNK,
    input_device_index=DEVICE_ID_3
)

start_time = datetime.now()

while True:
    try:
        before_time = datetime.now()
        frames1 = []
        frames2 = []
        frames3 = []
        for i in range(0, int(RATE / CHUNK * RECORD_SECONDS)):
            data1 = stream1.read(CHUNK, exception_on_overflow = False)
            data2 = stream2.read(CHUNK, exception_on_overflow = False)
            data3 = stream3.read(CHUNK, exception_on_overflow = False)
            frames1.append(data1)
            frames2.append(data2)
            frames3.append(data3)
        raw_data1 = b''.join(frames1)
        raw_data2 = b''.join(frames2)
        raw_data3 = b''.join(frames3)

        # produce asynchronously with callbacks
        producer.send('dataengineering', raw_data1, key="audio".encode('utf-8'))\
                .add_callback(on_send_success)\
                .add_errback(on_send_error)
        print("%s, audio_duration (s): %s, raw_data1" % (datetime.now().strftime("%d/%m/%Y, %H:%M:%S"), (datetime.now() - before_time).seconds))
        producer.send('dataengineering', raw_data2, key="audio".encode('utf-8'))\
                .add_callback(on_send_success)\
                .add_errback(on_send_error)
        print("%s audio_duration (s): %s, raw_data2" % (datetime.now().strftime("%d/%m/%Y, %H:%M:%S"), (datetime.now() - before_time).seconds))
        producer.send('dataengineering', raw_data3, key="audio".encode('utf-8'))\
                .add_callback(on_send_success)\
                .add_errback(on_send_error)
        print("%s audio_duration (s): %s, raw_data3" % (datetime.now().strftime("%d/%m/%Y, %H:%M:%S"), (datetime.now() - before_time).seconds))

        # block until all async messages are sent
        producer.flush()

        if (datetime.now() - start_time).seconds > 60: #exit program after 1min
            stream1.stop_stream()
            stream1.close()
            stream2.stop_stream()
            stream2.close()
            stream3.stop_stream()
            stream3.close()
            audio.terminate()
            print("* Exit program after 1min *")
            break
        
    except KeyboardInterrupt as kie:
        print("* Program terminated by user *")
        stream1.stop_stream()
        stream1.close()
        stream2.stop_stream()
        stream2.close()
        stream3.stop_stream()
        stream3.close()
        audio.terminate()
        break
    except Exception as e:
        # print("Exception:", e)
        if stream!=None:
            stream1.stop_stream()
            stream1.close()
            stream2.stop_stream()
            stream2.close()
            stream3.stop_stream()
            stream3.close()
            audio.terminate()
        break
```

# Conclusion¶

1. You have successfully streamed audio data from your device from a paragraph, directly and reliably transcribed the audio to text.
2. You have sent the audio data from one computer to another computer to be read and processed.

**Questions to ponder**

1. Which principle in the good data achitecture does Kafka fulfill?
2. Can Microsoft Power App exercises do stream processing?
3. What is the advantages and disadvantages of using stream processing?

# Submissions next Wed 9pm (6 Nov 2024)¶

Submit your ipynb as a pdf. Save your ipynb as a html file, open in browser and print as a pdf. Include in your submission:

```
Section 4.2

Answer the questions to ponder.
```

~ The End ~