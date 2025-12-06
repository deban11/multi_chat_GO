from PIL import Image
import os
import io

def process_photo(input_path, output_path):
    # 1. Load the image
    img = Image.open(input_path)
    
    # Convert to RGB if it's RGBA (removes transparency if any)
    if img.mode in ("RGBA", "P"):
        img = img.convert("RGB")

    # 2. Resize exactly to 200x230 pixels as per guidelines
    # Uses LANCZOS for high quality downsampling
    img_resized = img.resize((200, 230), Image.Resampling.LANCZOS)
    
    # 3. Save loop to ensure file size is 20KB - 50KB
    # We start at high quality because 200x230 is small dimensions
    quality = 100 
    target_min = 20 * 1024 # 20KB in bytes
    target_max = 50 * 1024 # 50KB in bytes
    
    while quality > 5:
        # Save to a memory buffer first to check size
        buffer = io.BytesIO()
        img_resized.save(buffer, format="JPEG", quality=quality, subsampling=0)
        size = buffer.tell()
        
        if target_min <= size <= target_max:
            with open(output_path, "wb") as f:
                f.write(buffer.getbuffer())
            print(f"Success! Saved to {output_path}")
            print(f"Final Dimensions: 200x230")
            print(f"Final Size: {size/1024:.2f} KB (Qual: {quality})")
            return
            
        elif size > target_max:
            # File too big, lower quality
            quality -= 5
        else:
            # File too small (<20KB). This is common for 200x230 images.
            # We must increase metadata or save at max quality 
            # (If 100% quality is still < 20KB, the portal might be strict)
            print(f"Warning: At 100% quality, file is only {size/1024:.2f} KB.")
            print("Trying to save with maximum settings...")
            with open(output_path, "wb") as f:
                 img_resized.save(f, format="JPEG", quality=100, subsampling=0)
            return

    print("Could not fit image into size constraints.")

# Usage: Replace 'myimage.jpeg' with your local file path
process_photo("myimage.jpeg", "upload_ready_photo.jpg")