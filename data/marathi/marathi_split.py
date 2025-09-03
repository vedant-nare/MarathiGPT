from datasets import load_dataset

# Load Marathi dataset as an iterable to save memory
dataset = load_dataset("ai4bharat/IndicCorpV2", "indiccorp_v2", streaming=True)
marathi_stream = dataset['mar_Deva']

#dataset = load_dataset("ai4bharat/IndicCorpV2", "mr", split="train", streaming=True)

BATCH_SIZE = 1000
buffer = []

with open("data/marathi/marathi.txt", "w", encoding="utf-8") as f:
    for row in marathi_stream:
        text = row.get("text")
        if text:
            buffer.append(text + "\n")
            if len(buffer) >= BATCH_SIZE:
                f.writelines([line if line.endswith('\n') else line + '\n' for line in buffer])
                buffer.clear()
        else:
            print("Warning: 'text' field missing in row:", row)
    if buffer:
        f.writelines(buffer)