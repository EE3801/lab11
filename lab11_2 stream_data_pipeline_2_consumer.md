# Lab 11.2 Stream Data Pipeline II - Consumer¶

- Scenario: Streaming audio 

Stream in audio, process, calling a machine learning classification model and save the data for reporting.

Create a new jupyter notebook file "stream\_data\_pipeline\_2\_consumer.ipynb".

# 1. Load whisper¶

In [ ]:

```
import whisper
model = whisper.load_model("medium.en") 
```

In [ ]:

```
# !pip install pandas
# !pip install -U scikit-learn
# !pip install nltk
# !pip install matplotlib
# !pip install sentence_transformers
```

# 2. Consume audio stream and transcribe¶

# 2.1 Initialise Consumer¶

In [ ]:

```
# kafka-python Consumer
from kafka import KafkaConsumer
import json
import numpy as np
from datetime import datetime
import sys

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('dataengineering',
                        #  group_id='python-consumer',
                         bootstrap_servers=['localhost:29092','localhost:39092','localhost:49092'])
                        #  consumer_timeout_ms=1000)
                         #value_deserializer=lambda m: json.loads(m.decode('utf-8')))

start_time = datetime.now()

try: 
    for message in consumer:
        begin_time = datetime.now()
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print("%s %s:%d:%d: key=%s" % (datetime.now().strftime("%d/%m/%Y, %H:%M:%S"), message.topic, message.partition,
                                            message.offset, message.key.decode('utf-8')))

        if message.key.decode('utf-8')=="text":
            print("message=%s" % message.value.decode('utf-8'))

        if message.key.decode('utf-8')=="audio":
            audio_data = np.frombuffer(message.value, dtype=np.int16).flatten().astype(np.float32) / 32768.0
            audio_data = whisper.pad_or_trim(audio_data)
            before_transcribe_time = datetime.now()
            text = whisper.transcribe(model, audio_data, fp16=False)["text"]
            print("transcribed.message=%s, transcribed.duration=%s" % (text, 
                                                                       str(datetime.now()-before_transcribe_time)))

        if (datetime.now()-start_time).seconds > 60: # listen for 1 minute (60 seconds)
            consumer.close()
            print("* Ended listening for 1 min *")
            break
except KeyboardInterrupt as kie:
    consumer.close()
    print("* Program terminated by user *")

```

# 3. Consume audio stream, transcribe and identify number of speakers¶

# 3.1 Detect speakers¶

In [ ]:

```
# !pip3 install pyannote.audio
# !pip3 install torch torchvision torchaudio
```

In [ ]:

```
### diarization - https://github.com/pyannote/pyannote-audio/tree/develop?tab=readme-ov-file
from pyannote.audio import Pipeline
import torchaudio
import torch
import pyaudio

DEVICE_ID = 1
audio = pyaudio.PyAudio()
RATE = int(audio.get_device_info_by_index(DEVICE_ID)['defaultSampleRate'])
CHANNELS = int(audio.get_device_info_by_index(DEVICE_ID)['maxInputChannels'])
audio.terminate()

speaker_list = []
waveform = []

def detect_speakers(audio_data_):
    pipeline = Pipeline.from_pretrained(
        "pyannote/speaker-diarization-3.1",
        use_auth_token="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")

    # send pipeline to GPU (when available)
    pipeline.to(torch.device("cpu"))

    # apply pretrained pipeline
    # waveform, sample_rate = torchaudio.load("output.wav")
    waveform.append(audio_data_)
    # audio_data_ = torch.from_numpy(audio_data_).float().unsqueeze(dim=0).to(device=torch.device('cpu'))
    # print("audio_data_.shape:",audio_data_.shape[0])
    this_waveform = torch.from_numpy(np.array(waveform)).float().to(device=torch.device('cpu'))

    diarization = pipeline({"waveform": this_waveform, "sample_rate": RATE})

    # print the result
    for turn, _, speaker in diarization.itertracks(yield_label=True):
        print(f"start={turn.start:.1f}s stop={turn.end:.1f}s speaker_{speaker}")
        speaker_list.append(speaker)

    return diarization
        
```

In [ ]:

```
# kafka-python Consumer
from kafka import KafkaConsumer
import json
import numpy as np
from datetime import datetime
import sys

# To consume latest messages and auto-commit offsets
consumer = KafkaConsumer('dataengineering',
                        #  group_id='python-consumer',
                         bootstrap_servers=['localhost:29092','localhost:39092','localhost:49092'])
                        #  consumer_timeout_ms=1000)
                         #value_deserializer=lambda m: json.loads(m.decode('utf-8')))

start_time = datetime.now()

try: 
    for message in consumer:
        begin_time = datetime.now()
        # message value and key are raw bytes -- decode if necessary!
        # e.g., for unicode: `message.value.decode('utf-8')`
        print("%s %s:%d:%d: key=%s" % (datetime.now().strftime("%d/%m/%Y, %H:%M:%S"), message.topic, message.partition,
                                            message.offset, message.key.decode('utf-8')))

        if message.key.decode('utf-8')=="text":
            print("message=%s" % message.value.decode('utf-8'))

        if message.key.decode('utf-8')=="audio":
            audio_data = np.frombuffer(message.value, dtype=np.int16).flatten().astype(np.float32) / 32768.0
            audio_data = whisper.pad_or_trim(audio_data)
            before_transcribe_time = datetime.now()
            text = whisper.transcribe(model, audio_data, fp16=False)["text"]
            print("transcribed.message=%s, transcribed.duration=%s" % (text, 
                                                                       str(datetime.now()-before_transcribe_time)))
            # detect speakers
            dia = detect_speakers(audio_data)
            print("Number of speakers detected:",len(list(dict.fromkeys(speaker_list))))

        if (datetime.now()-start_time).seconds > 60: # listen for 1 minute (60 seconds)
            consumer.close()
            print("* Ended listening for 1 min *")
            break
except KeyboardInterrupt as kie:
    consumer.close()
    print("* Program terminated by user *")

```

In [ ]:

```
dia
```

In [ ]:

```
import matplotlib.pyplot as plt

for wave in waveform:
    plt.plot(np.array(wave))
    plt.show()
```

~ The End ~