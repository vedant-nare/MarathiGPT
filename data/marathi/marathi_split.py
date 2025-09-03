from datasets import load_dataset
import random

# Load Marathi dataset as streaming iterable
dataset = load_dataset("ai4bharat/IndicCorpV2", "indiccorp_v2", streaming=True)
marathi_stream = dataset['mar_Deva']

MAX_LINES = 50_000 # MAX_LINES = 50_000 (~10MB) → for laptop. MAX_LINES = 200_000 (~40MB) → for Colab T4
BATCH_SIZE = 1000

buffer = []
line_count = 0

with open("data/marathi/marathi.txt", "w", encoding="utf-8") as f:
    # Reservoir sampling for random subset
    reservoir = []
    for i, row in enumerate(marathi_stream):
        text = row.get("text")
        if not text:
            continue

        cleaned = text.strip()
        if not cleaned:
            continue

        if len(reservoir) < MAX_LINES:
            reservoir.append(cleaned)
        else:
            j = random.randint(0, i)
            if j < MAX_LINES:
                reservoir[j] = cleaned

    # Now reservoir has random MAX_LINES samples
    for line in reservoir:
        buffer.append(line + "\n")
        line_count += 1
        if len(buffer) >= BATCH_SIZE:
            f.writelines(buffer)
            buffer.clear()

    if buffer:
        f.writelines(buffer)

print(f"✅ Randomly saved {line_count} lines to data/marathi/marathi.txt")
